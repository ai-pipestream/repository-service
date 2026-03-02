package ai.pipestream.repository.grpc;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.repository.account.AccountCacheService;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.filesystem.v1.*;
import ai.pipestream.repository.kafka.RepositoryEventEmitter;
import ai.pipestream.repository.s3.S3Config;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.service.DriveService;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.protobuf.util.Timestamps.fromMillis;

/**
 * Filesystem service implementation.
 * Provides drive CRUD, S3 admin ops, search, reindex, and metadata streaming.
 */
@GrpcService
public class FilesystemGrpcService extends MutinyFilesystemServiceGrpc.FilesystemServiceImplBase {

    private static final Logger LOG = Logger.getLogger(FilesystemGrpcService.class);

    @Inject
    DocumentStorageService storageService;

    @Inject
    DriveService driveService;

    @Inject
    AccountCacheService accountCacheService;

    @Inject
    S3Config s3Config;

    @Inject
    S3AsyncClient s3AsyncClient;

    @Inject
    @ProtobufChannel("drive-updates-out")
    ProtobufEmitter<DriveUpdateNotification> driveUpdateEmitter;

    @Inject
    RepositoryEventEmitter repositoryEventEmitter;

    // ---- CreateDrive ----

    @Override
    @WithSession
    public Uni<CreateDriveResponse> createDrive(CreateDriveRequest request) {
        String name = request.getName();
        String accountId = request.getAccountId();

        if (name == null || name.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("name is required").asRuntimeException());
        }
        if (accountId == null || accountId.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("account_id is required").asRuntimeException());
        }
        if (name.contains(":")) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("drive name must not contain ':'").asRuntimeException());
        }

        // Validate account exists
        return accountCacheService.isValidAccount(accountId)
                .flatMap(isValid -> {
                    if (!isValid) {
                        return Uni.createFrom().failure(
                                Status.NOT_FOUND.withDescription("Account not found or inactive: " + accountId).asRuntimeException());
                    }

                    String bucketName = (request.getBucketName() != null && !request.getBucketName().isBlank())
                            ? request.getBucketName()
                            : s3Config.bucket();

                    ai.pipestream.repository.entity.Drive drive = new ai.pipestream.repository.entity.Drive();
                    drive.driveId = accountId + ":" + name;
                    drive.name = name;
                    drive.accountId = accountId;
                    drive.s3Bucket = bucketName;
                    drive.s3Prefix = accountId;
                    drive.region = request.getRegion().isBlank() ? null : request.getRegion();
                    drive.description = request.getDescription().isBlank() ? null : request.getDescription();
                    drive.metadata = request.getMetadata().isBlank() ? null : request.getMetadata();
                    drive.createdAt = Instant.now();
                    drive.updatedAt = Instant.now();

                    // Best-effort bucket creation if requested
                    Uni<Void> bucketCreation = Uni.createFrom().voidItem();
                    if (request.getCreateBucket() && !bucketName.equals(s3Config.bucket())) {
                        bucketCreation = Uni.createFrom().completionStage(
                                s3AsyncClient.createBucket(
                                        software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                                                .bucket(bucketName)
                                                .build())
                        ).replaceWithVoid()
                        .onFailure().recoverWithUni(e -> {
                            LOG.warnf("Best-effort bucket creation failed for %s: %s", bucketName, e.getMessage());
                            return Uni.createFrom().voidItem();
                        });
                    }

                    return bucketCreation.flatMap(v -> driveService.createDrive(drive));
                })
                .map(persisted -> {
                    emitDriveUpdate("CREATED", persisted);
                    return CreateDriveResponse.newBuilder()
                            .setDrive(toProtoDrive(persisted))
                            .build();
                });
    }

    // ---- UpdateDrive ----

    @Override
    @WithSession
    public Uni<UpdateDriveResponse> updateDrive(UpdateDriveRequest request) {
        String driveName = request.getDriveName();
        if (driveName == null || driveName.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("drive_name is required").asRuntimeException());
        }

        return driveService.findByDriveId(driveName)
                .flatMap(drive -> {
                    if (drive == null) {
                        return Uni.createFrom().failure(
                                Status.NOT_FOUND.withDescription("Drive not found: " + driveName).asRuntimeException());
                    }

                    // Apply non-blank fields
                    if (!request.getDescription().isBlank()) {
                        drive.description = request.getDescription();
                    }
                    if (!request.getMetadata().isBlank()) {
                        drive.metadata = request.getMetadata();
                    }

                    // Credentials/KMS: log warning and skip (no Infisical yet)
                    if (request.hasS3Credentials()) {
                        LOG.warnf("UpdateDrive: s3_credentials provided for %s but Infisical integration not yet available, skipping", driveName);
                    }
                    if (request.hasKmsEncryption()) {
                        LOG.warnf("UpdateDrive: kms_encryption provided for %s but Infisical integration not yet available, skipping", driveName);
                    }

                    drive.updatedAt = Instant.now();

                    return Panache.withTransaction(() -> drive.<ai.pipestream.repository.entity.Drive>persist());
                })
                .map(persisted -> {
                    driveService.invalidateCache(persisted.driveId);
                    emitDriveUpdate("UPDATED", persisted);
                    return UpdateDriveResponse.newBuilder()
                            .setDrive(toProtoDrive(persisted))
                            .build();
                });
    }

    // ---- GetDrive ----

    @Override
    @WithSession
    public Uni<GetDriveResponse> getDrive(GetDriveRequest request) {
        String name = request.getName();
        if (name == null || name.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("name (drive_id) is required").asRuntimeException());
        }

        return driveService.findByDriveId(name)
                .map(drive -> {
                    if (drive == null) {
                        throw Status.NOT_FOUND.withDescription("Drive not found: " + name).asRuntimeException();
                    }
                    return GetDriveResponse.newBuilder()
                            .setDrive(toProtoDrive(drive))
                            .build();
                });
    }

    // ---- ListDrives ----

    @Override
    @WithSession
    public Uni<ListDrivesResponse> listDrives(ListDrivesRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int parsedPage = 0;
        if (request.getPageToken() != null && !request.getPageToken().isEmpty()) {
            try {
                parsedPage = Integer.parseInt(request.getPageToken());
            } catch (NumberFormatException e) {
                LOG.warnf("Invalid page token: %s", request.getPageToken());
            }
        }
        final int page = parsedPage;

        // Support filter field for account_id filtering
        String accountIdFilter = null;
        String filter = request.getFilter();
        if (filter != null && !filter.isBlank()) {
            if (filter.startsWith("account_id=")) {
                accountIdFilter = filter.substring("account_id=".length()).trim();
            }
        }

        Uni<Long> countUni;
        Uni<List<ai.pipestream.repository.entity.Drive>> drivesUni;

        if (accountIdFilter != null && !accountIdFilter.isBlank()) {
            final String acctFilter = accountIdFilter;
            countUni = ai.pipestream.repository.entity.Drive.count("accountId", acctFilter);
            drivesUni = ai.pipestream.repository.entity.Drive.<ai.pipestream.repository.entity.Drive>find("accountId", acctFilter)
                    .page(page, pageSize)
                    .list();
        } else {
            countUni = ai.pipestream.repository.entity.Drive.count();
            drivesUni = ai.pipestream.repository.entity.Drive.<ai.pipestream.repository.entity.Drive>findAll()
                    .page(page, pageSize)
                    .list();
        }

        return Uni.combine().all().unis(countUni, drivesUni)
                .asTuple()
                .map(tuple -> {
                    long totalCount = tuple.getItem1();
                    List<ai.pipestream.repository.entity.Drive> drives = tuple.getItem2();

                    ListDrivesResponse.Builder responseBuilder = ListDrivesResponse.newBuilder()
                            .setTotalCount((int) totalCount);

                    for (ai.pipestream.repository.entity.Drive d : drives) {
                        responseBuilder.addDrives(toProtoDrive(d));
                    }

                    if ((page + 1) * pageSize < totalCount) {
                        responseBuilder.setNextPageToken(String.valueOf(page + 1));
                    }

                    return responseBuilder.build();
                });
    }

    // ---- DeleteDrive ----

    @Override
    @WithSession
    public Uni<DeleteDriveResponse> deleteDrive(DeleteDriveRequest request) {
        String name = request.getName();
        String confirmation = request.getConfirmation();

        if (name == null || name.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("name is required").asRuntimeException());
        }
        if (!"DELETE_DRIVE_DATA".equals(confirmation)) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("confirmation must be 'DELETE_DRIVE_DATA'").asRuntimeException());
        }

        return driveService.findByDriveId(name)
                .flatMap(drive -> {
                    if (drive == null) {
                        return Uni.createFrom().failure(
                                Status.NOT_FOUND.withDescription("Drive not found: " + name).asRuntimeException());
                    }

                    return driveService.deleteDrive(drive)
                            .map(deletedCount -> {
                                emitDriveUpdate("DELETED", drive);
                                return DeleteDriveResponse.newBuilder()
                                        .setSuccess(true)
                                        .setMessage("Drive deleted: " + name)
                                        .setNodesDeleted((int) (long) deletedCount)
                                        .build();
                            });
                });
    }

    // ---- ListDriveBucketStatus ----

    @Override
    @WithSession
    public Uni<ListDriveBucketStatusResponse> listDriveBucketStatus(ListDriveBucketStatusRequest request) {
        return ai.pipestream.repository.entity.Drive.<ai.pipestream.repository.entity.Drive>listAll()
                .flatMap(drives -> {
                    if (drives.isEmpty()) {
                        return Uni.createFrom().item(
                                ListDriveBucketStatusResponse.newBuilder().setTotal(0).build());
                    }

                    List<Uni<DriveBucketStatus>> statusUnis = new ArrayList<>();
                    for (ai.pipestream.repository.entity.Drive drive : drives) {
                        String bucket = drive.s3Bucket != null ? drive.s3Bucket : s3Config.bucket();
                        Uni<DriveBucketStatus> statusUni = Uni.createFrom().completionStage(
                                s3AsyncClient.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
                        ).map(resp -> DriveBucketStatus.newBuilder()
                                .setAlias(drive.driveId)
                                .setBucketName(bucket)
                                .setAccess(DriveBucketStatus.BucketAccess.BUCKET_ACCESS_OK)
                                .build()
                        ).onFailure().recoverWithItem(e -> DriveBucketStatus.newBuilder()
                                .setAlias(drive.driveId)
                                .setBucketName(bucket)
                                .setAccess(DriveBucketStatus.BucketAccess.BUCKET_ACCESS_ERROR)
                                .setErrorMessage(e.getMessage())
                                .build());

                        statusUnis.add(statusUni);
                    }

                    return Uni.join().all(statusUnis).andCollectFailures()
                            .map(statuses -> {
                                ListDriveBucketStatusResponse.Builder builder =
                                        ListDriveBucketStatusResponse.newBuilder()
                                                .setTotal(statuses.size());
                                for (DriveBucketStatus s : statuses) {
                                    builder.addItems(s);
                                }
                                return builder.build();
                            });
                });
    }

    // ---- CreateBucket ----

    @Override
    public Uni<CreateBucketResponse> createBucket(ai.pipestream.repository.filesystem.v1.CreateBucketRequest request) {
        String bucketName = request.getBucketName();
        if (bucketName == null || bucketName.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("bucket_name is required").asRuntimeException());
        }

        return Uni.createFrom().completionStage(
                s3AsyncClient.createBucket(
                        software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                                .bucket(bucketName)
                                .build())
        ).map(resp -> CreateBucketResponse.newBuilder()
                .setSuccess(true)
                .setBucketName(bucketName)
                .setMessage("Bucket created: " + bucketName)
                .build()
        ).onFailure().recoverWithItem(e -> CreateBucketResponse.newBuilder()
                .setSuccess(false)
                .setBucketName(bucketName)
                .setMessage("Failed to create bucket: " + e.getMessage())
                .build());
    }

    // ---- GetFilesystemNode ----

    @Override
    @WithSession
    public Uni<GetFilesystemNodeResponse> getFilesystemNode(GetFilesystemNodeRequest request) {
        if (request == null || request.getDocumentId() == null || request.getDocumentId().isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("document_id is required").asRuntimeException());
        }

        String accountId = request.getDrive();
        String documentId = request.getDocumentId();

        return storageService.findDocumentById(documentId)
                .map(records -> selectLatest(records, accountId))
                .map(optionalRecord -> optionalRecord.orElseThrow(() ->
                        Status.NOT_FOUND.withDescription("Document not found: " + documentId).asRuntimeException()))
                .map(this::toNode)
                .map(node -> GetFilesystemNodeResponse.newBuilder().setNode(node).build());
    }

    // ---- SearchDrives ----

    @Override
    @WithSession
    public Uni<SearchDrivesResponse> searchDrives(SearchDrivesRequest request) {
        long startTime = System.currentTimeMillis();
        String query = request.getQuery();

        if (query == null || query.isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("query is required").asRuntimeException());
        }

        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());
        String likePattern = "%" + query.toLowerCase() + "%";

        String hql = "lower(name) LIKE ?1 OR lower(description) LIKE ?1 OR lower(driveId) LIKE ?1";

        Uni<Long> countUni = ai.pipestream.repository.entity.Drive.count(hql, likePattern);
        Uni<List<ai.pipestream.repository.entity.Drive>> drivesUni =
                ai.pipestream.repository.entity.Drive.<ai.pipestream.repository.entity.Drive>find(hql, likePattern)
                        .page(page, pageSize)
                        .list();

        return Uni.combine().all().unis(countUni, drivesUni)
                .asTuple()
                .map(tuple -> {
                    long totalCount = tuple.getItem1();
                    List<ai.pipestream.repository.entity.Drive> drives = tuple.getItem2();
                    long tookMillis = System.currentTimeMillis() - startTime;

                    SearchDrivesResponse.Builder builder = SearchDrivesResponse.newBuilder()
                            .setTotalCount((int) totalCount)
                            .setTookMillis(tookMillis);

                    for (ai.pipestream.repository.entity.Drive d : drives) {
                        builder.addResults(DriveSearchResult.newBuilder()
                                .setDrive(toProtoDrive(d))
                                .setScore(1.0)
                                .build());
                    }

                    if ((page + 1) * pageSize < totalCount) {
                        builder.setNextPageToken(String.valueOf(page + 1));
                    }

                    return builder.build();
                });
    }

    // ---- SearchNodes ----

    @Override
    @WithSession
    public Uni<SearchNodesResponse> searchNodes(SearchNodesRequest request) {
        long startTime = System.currentTimeMillis();
        String query = request.getQuery();
        String driveFilter = request.getDrive();
        List<String> pathFilters = request.getPathsList();

        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());

        // Build dynamic HQL
        StringBuilder hql = new StringBuilder("1=1");
        List<Object> params = new ArrayList<>();
        int paramIndex = 1;

        if (driveFilter != null && !driveFilter.isBlank()) {
            hql.append(" AND driveName = ?").append(paramIndex);
            params.add(driveFilter);
            paramIndex++;
        }

        if (query != null && !query.isBlank()) {
            String likePattern = "%" + query.toLowerCase() + "%";
            hql.append(" AND (lower(docId) LIKE ?").append(paramIndex)
               .append(" OR lower(filename) LIKE ?").append(paramIndex)
               .append(" OR lower(objectKey) LIKE ?").append(paramIndex)
               .append(")");
            params.add(likePattern);
            paramIndex++;
        }

        if (pathFilters != null && !pathFilters.isEmpty()) {
            hql.append(" AND (");
            for (int i = 0; i < pathFilters.size(); i++) {
                if (i > 0) hql.append(" OR ");
                hql.append("objectKey LIKE ?").append(paramIndex);
                params.add(pathFilters.get(i) + "%");
                paramIndex++;
            }
            hql.append(")");
        }

        String hqlStr = hql.toString();
        Object[] paramsArr = params.toArray();

        Uni<Long> countUni = PipeDocRecord.count(hqlStr, paramsArr);
        Uni<List<PipeDocRecord>> recordsUni =
                PipeDocRecord.<PipeDocRecord>find(hqlStr, paramsArr)
                        .page(page, pageSize)
                        .list();

        return Uni.combine().all().unis(countUni, recordsUni)
                .asTuple()
                .map(tuple -> {
                    long totalCount = tuple.getItem1();
                    List<PipeDocRecord> records = tuple.getItem2();
                    long tookMillis = System.currentTimeMillis() - startTime;

                    SearchNodesResponse.Builder builder = SearchNodesResponse.newBuilder()
                            .setTotalCount((int) totalCount)
                            .setTookMillis(tookMillis);

                    for (PipeDocRecord r : records) {
                        builder.addNodes(SearchResult.newBuilder()
                                .setNode(toNode(r))
                                .setScore(1.0)
                                .build());
                    }

                    if ((page + 1) * pageSize < totalCount) {
                        builder.setNextPageToken(String.valueOf(page + 1));
                    }

                    return builder.build();
                });
    }

    // ---- ReindexPipeDocs ----

    @Override
    @WithSession
    public Uni<ReindexPipeDocsResponse> reindexPipeDocs(ReindexPipeDocsRequest request) {
        String driveFilter = request.getDrive();
        int limit = request.getLimit();
        boolean dryRun = request.getDryRun();

        // Build query
        String hql = (driveFilter != null && !driveFilter.isBlank())
                ? "driveName = ?1"
                : null;

        Uni<List<PipeDocRecord>> recordsUni;
        if (hql != null) {
            var query = PipeDocRecord.<PipeDocRecord>find(hql, driveFilter);
            if (limit > 0) {
                query = query.page(0, limit);
            }
            recordsUni = query.list();
        } else {
            var query = PipeDocRecord.<PipeDocRecord>findAll();
            if (limit > 0) {
                query = query.page(0, limit);
            }
            recordsUni = query.list();
        }

        return recordsUni.flatMap(records -> {
            int scanned = records.size();

            if (dryRun) {
                return Uni.createFrom().item(ReindexPipeDocsResponse.newBuilder()
                        .setScanned(scanned)
                        .setReindexed(0)
                        .setErrors(0)
                        .build());
            }

            // Pre-resolve buckets for each unique driveName
            Map<String, String> driveToBucket = new HashMap<>();
            AtomicInteger reindexed = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger(0);

            for (PipeDocRecord record : records) {
                try {
                    String bucket = driveToBucket.computeIfAbsent(record.driveName, dn -> {
                        DriveService.ResolvedDrive rd = driveService.resolveDrive(dn, record.accountId)
                                .await().indefinitely();
                        return rd.bucket();
                    });

                    repositoryEventEmitter.emitCreated(
                            record.docId,
                            record.accountId,
                            record.objectKey,
                            record.pipedocObjectKey,
                            record.sizeBytes != null ? record.sizeBytes : 0L,
                            record.checksum,
                            bucket,
                            record.versionId,
                            UUID.randomUUID().toString(),
                            record.connectorId,
                            record.datasourceId
                    );
                    reindexed.incrementAndGet();
                } catch (Exception e) {
                    LOG.warnf("ReindexPipeDocs: failed to re-emit event for docId=%s: %s",
                            record.docId, e.getMessage());
                    errors.incrementAndGet();
                }
            }

            return Uni.createFrom().item(ReindexPipeDocsResponse.newBuilder()
                    .setScanned(scanned)
                    .setReindexed(reindexed.get())
                    .setErrors(errors.get())
                    .build());
        });
    }

    // ---- StreamAllMetadata ----

    @Override
    @WithSession
    public Multi<StreamAllMetadataResponse> streamAllMetadata(StreamAllMetadataRequest request) {
        boolean includeDrives = request.getIncludeDrives();
        boolean includeNodes = request.getIncludeNodes();
        String driveFilter = request.getDrive();
        long batchSize = request.getBatchSize() > 0 ? request.getBatchSize() : 1000;
        boolean hasSince = request.hasSince();
        Instant since = hasSince
                ? Instant.ofEpochSecond(request.getSince().getSeconds(), request.getSince().getNanos())
                : null;

        AtomicLong sequenceCounter = new AtomicLong(0);

        // Drive stream
        Multi<StreamAllMetadataResponse> driveStream;
        if (includeDrives) {
            driveStream = Multi.createFrom().uni(
                    Uni.createFrom().deferred(() -> {
                        if (driveFilter != null && !driveFilter.isBlank()) {
                            return ai.pipestream.repository.entity.Drive
                                    .<ai.pipestream.repository.entity.Drive>find("driveId", driveFilter).list();
                        }
                        return ai.pipestream.repository.entity.Drive.<ai.pipestream.repository.entity.Drive>listAll();
                    })
            ).onItem().transformToMulti(drives -> Multi.createFrom().iterable(drives))
             .concatenate()
             .filter(d -> since == null || (d.updatedAt != null && d.updatedAt.isAfter(since)))
             .map(d -> StreamAllMetadataResponse.newBuilder()
                     .setDrive(DriveMetadata.newBuilder()
                             .setDrive(toProtoDrive(d))
                             .setS3Key(d.s3Bucket != null ? d.s3Bucket + "/" + (d.s3Prefix != null ? d.s3Prefix : "") : "")
                             .build())
                     .setSequenceNumber(sequenceCounter.incrementAndGet())
                     .build());
        } else {
            driveStream = Multi.createFrom().empty();
        }

        // Node stream (paginated iteration of PipeDocRecords)
        Multi<StreamAllMetadataResponse> nodeStream;
        if (includeNodes) {
            int pageSizeInt = (int) Math.min(batchSize, 1000);
            nodeStream = Multi.createBy().repeating()
                    .uni(AtomicInteger::new, pageState -> {
                        int currentPage = pageState.getAndIncrement();
                        return Uni.createFrom().deferred(() -> {
                            if (driveFilter != null && !driveFilter.isBlank()) {
                                if (since != null) {
                                    return PipeDocRecord.<PipeDocRecord>find(
                                            "driveName = ?1 AND createdAt > ?2", driveFilter, since)
                                            .page(currentPage, pageSizeInt).list();
                                }
                                return PipeDocRecord.<PipeDocRecord>find("driveName", driveFilter)
                                        .page(currentPage, pageSizeInt).list();
                            }
                            if (since != null) {
                                return PipeDocRecord.<PipeDocRecord>find("createdAt > ?1", since)
                                        .page(currentPage, pageSizeInt).list();
                            }
                            return PipeDocRecord.<PipeDocRecord>findAll()
                                    .page(currentPage, pageSizeInt).list();
                        });
                    })
                    .until(List::isEmpty)
                    .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
                    .concatenate()
                    .map(record -> StreamAllMetadataResponse.newBuilder()
                            .setNode(NodeMetadata.newBuilder()
                                    .setNode(toNode(record))
                                    .setDrive(record.driveName != null ? record.driveName : "")
                                    .setS3Key(record.pipedocObjectKey != null ? record.pipedocObjectKey : "")
                                    .setFullPath(record.objectKey != null ? record.objectKey : "")
                                    .build())
                            .setSequenceNumber(sequenceCounter.incrementAndGet())
                            .build());
        } else {
            nodeStream = Multi.createFrom().empty();
        }

        // Concatenate both streams; emit an empty-last marker if nothing was streamed
        Multi<StreamAllMetadataResponse> combined =
                Multi.createBy().concatenating().streams(driveStream, nodeStream);

        return combined.onCompletion().ifEmpty().continueWith(
                StreamAllMetadataResponse.newBuilder()
                        .setSequenceNumber(0)
                        .setIsLast(true)
                        .build());
    }

    // ---- Helpers ----

    private ai.pipestream.repository.filesystem.v1.Drive toProtoDrive(ai.pipestream.repository.entity.Drive d) {
        ai.pipestream.repository.filesystem.v1.Drive.Builder builder =
                ai.pipestream.repository.filesystem.v1.Drive.newBuilder();
        if (d.id != null) builder.setId(d.id);
        if (d.name != null) builder.setName(d.name);
        if (d.s3Bucket != null) builder.setBucketName(d.s3Bucket);
        if (d.accountId != null) builder.setAccountId(d.accountId);
        if (d.region != null) builder.setRegion(d.region);
        if (d.credentialsRef != null) builder.setCredentialsRef(d.credentialsRef);
        if (d.description != null) builder.setDescription(d.description);
        if (d.metadata != null) builder.setMetadata(d.metadata);
        if (d.createdAt != null) builder.setCreatedAt(fromMillis(d.createdAt.toEpochMilli()));
        return builder.build();
    }

    private void emitDriveUpdate(String updateType, ai.pipestream.repository.entity.Drive drive) {
        try {
            DriveUpdateNotification notification = DriveUpdateNotification.newBuilder()
                    .setUpdateType(updateType)
                    .setDrive(toProtoDrive(drive))
                    .setTimestamp(fromMillis(Instant.now().toEpochMilli()))
                    .build();
            driveUpdateEmitter.send(notification);
            LOG.infof("Emitted DriveUpdateNotification: type=%s, driveId=%s", updateType, drive.driveId);
        } catch (Exception e) {
            LOG.warnf("Failed to emit DriveUpdateNotification for %s: %s", drive.driveId, e.getMessage());
        }
    }

    private Optional<PipeDocRecord> selectLatest(List<PipeDocRecord> records, String accountId) {
        if (records == null || records.isEmpty()) {
            return Optional.empty();
        }

        return records.stream()
                .filter(record -> accountId == null || accountId.isBlank() || accountId.equals(record.accountId))
                .sorted(Comparator.comparing((PipeDocRecord r) -> r.createdAt != null ? r.createdAt : Instant.EPOCH).reversed())
                .findFirst()
                .or(() -> records.stream()
                        .sorted(Comparator.comparing((PipeDocRecord r) -> r.createdAt != null ? r.createdAt : Instant.EPOCH).reversed())
                        .findFirst());
    }

    private Node toNode(PipeDocRecord record) {
        Node.Builder nodeBuilder = Node.newBuilder()
                .setDocumentId(record.docId == null ? "" : record.docId)
                .setName(record.filename == null ? "" : record.filename)
                .setPath(record.objectKey == null ? "" : record.objectKey)
                .setContentType(record.contentType == null ? "" : record.contentType)
                .setSizeBytes(record.sizeBytes == null ? 0L : record.sizeBytes)
                .setS3Key(record.pipedocObjectKey == null ? "" : record.pipedocObjectKey)
                .setType(Node.NodeType.NODE_TYPE_FILE);

        if (record.createdAt != null) {
            nodeBuilder.setCreatedAt(fromMillis(record.createdAt.toEpochMilli()));
        }

        return nodeBuilder.build();
    }

    private int parsePageToken(String pageToken) {
        if (pageToken != null && !pageToken.isEmpty()) {
            try {
                return Integer.parseInt(pageToken);
            } catch (NumberFormatException e) {
                LOG.warnf("Invalid page token: %s", pageToken);
            }
        }
        return 0;
    }
}
