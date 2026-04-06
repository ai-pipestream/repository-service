package ai.pipestream.repository.service;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.account.AccountCacheService;
import ai.pipestream.repository.entity.Document;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.kafka.RepositoryEventEmitter;
import ai.pipestream.repository.s3.S3Config;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Service for managing document storage operations.
 * Handles CRUD operations for documents in the repository.
 *
 * Sequential flatMap chain is used within transactions to ensure Hibernate Reactive session safety.
 */
@ApplicationScoped
public class DocumentStorageService {

    /**
     * Result of {@link #deleteLogicalDocument}; feeds gRPC {@code DeletePipeDocResponse} (counts, node ids, outcome).
     */
    public record LogicalDeleteResult(int pipedocsRemoved, List<UUID> removedNodeIds, boolean removedDocumentCatalogRowOnly) {
        public boolean nothingRemoved() {
            return pipedocsRemoved == 0 && removedNodeIds.isEmpty() && !removedDocumentCatalogRowOnly;
        }
    }

    private static final Logger LOG = Logger.getLogger(DocumentStorageService.class);
    private static final int MAX_PAGE_SIZE = 1000;
    private static final String DEFAULT_QUERY_CONDITION = "1=1";
    private static final String DEFAULT_ORDER_BY = " order by createdAt desc";

    /** Default retention hint (days) on repository events and pipedoc notifications for OpenSearch ILM. */
    public static final int DEFAULT_RETENTION_INTENT_DAYS = 30;

    @Inject S3AsyncClient s3AsyncClient;
    @Inject S3Config s3Config;
    @Inject AccountCacheService accountCacheService;
    @Inject RepositoryEventEmitter eventEmitter;
    @Inject io.vertx.mutiny.core.Vertx vertx;
    @Inject PipeDocUuidGenerator uuidGenerator;
    @Inject DriveService driveService;
    @Inject RedisStorageConfig redisStorageConfig;
    @Inject RedisDocumentCache redisCache;

    public DocumentStorageService() {
        LOG.info("DocumentStorageService initialized (Reactive)");
    }

    public Uni<StoredDocument> store(PipeDoc document) {
        return store(document, null, null, null, null);
    }

    public Uni<StoredDocument> store(PipeDoc document, String requestId) {
        return store(document, requestId, null, null, null);
    }

    public Uni<StoredDocument> store(PipeDoc document, String requestId, String graphLocationId, String clusterId) {
        return store(document, requestId, graphLocationId, clusterId, null);
    }

    public Uni<StoredDocument> store(PipeDoc document, String requestId, String graphLocationId, String clusterId, String driveName) {
        if (document == null) return Uni.createFrom().failure(new IllegalArgumentException("document must not be null"));
        if (!document.hasOwnership()) return Uni.createFrom().failure(new IllegalArgumentException("document must have ownership context"));

        OwnershipContext ownership = document.getOwnership();
        String accountId = ownership.getAccountId();
        if (accountId == null || accountId.isBlank()) return Uni.createFrom().failure(new IllegalArgumentException("ownership.account_id is required"));

        // Capture Vertx context — account validation gRPC callback may complete
        // on a non-Vertx thread, and downstream Panache calls require it.
        io.vertx.core.Context callerContext = io.vertx.core.Vertx.currentContext();

        return accountCacheService.isValidAccount(accountId)
                .emitOn(runnable -> {
                    if (callerContext != null) callerContext.runOnContext(v -> runnable.run());
                    else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                })
                .flatMap(isValid -> {
                    if (!isValid) return Uni.createFrom().failure(new AccountValidationException("Account not found or inactive: " + accountId));

                    String documentId = document.getDocId();
                    PipeDoc docToStore = (documentId == null || documentId.isBlank()) 
                            ? document.toBuilder().setDocId(UUID.randomUUID().toString()).build() : document;
                    
                    final String finalDocId = docToStore.getDocId();
                    String connectorId = ownership.getConnectorId();
                    String finalDatasourceId = (ownership.getDatasourceId() == null || ownership.getDatasourceId().isBlank())
                            ? computeDatasourceId(accountId, connectorId) : ownership.getDatasourceId();

                    String resolvedRequestId = (requestId == null || requestId.isBlank()) ? UUID.randomUUID().toString() : requestId;
                    String finalGraphAddressId = (graphLocationId != null && !graphLocationId.isBlank()) ? graphLocationId : finalDatasourceId;

                    return driveService.resolveDrive(driveName, accountId)
                            .flatMap(resolvedDrive -> continueStoring(docToStore, finalDocId, accountId, connectorId, finalDatasourceId,
                                    resolvedRequestId, finalGraphAddressId, clusterId, resolvedDrive));
                });
    }

    private Uni<StoredDocument> continueStoring(PipeDoc docToStore, String finalDocId, String accountId,
            String connectorId, String finalDatasourceId, String resolvedRequestId, String resolvedGraphAddressId,
            String clusterId, DriveService.ResolvedDrive resolvedDrive) {
        Context requestContext = Vertx.currentContext().getDelegate();

        String driveName = resolvedDrive.driveId();
        String resolvedBucket = resolvedDrive.bucket();
        String resolvedKeyPrefix = resolvedDrive.keyPrefix();
        
        byte[] pipeDocBytes = docToStore.toByteArray();
        String contentType = "application/x-protobuf";
        long sizeBytes = pipeDocBytes.length;
        String checksum = computeSha256(pipeDocBytes);

        UUID nodeId = uuidGenerator.generateNodeId(finalDocId, resolvedGraphAddressId, accountId);
        String basePath = buildObjectKeyBase(resolvedKeyPrefix, driveName, accountId, connectorId, finalDatasourceId, finalDocId, clusterId);
        String completeObjectKey = basePath + "/" + nodeId.toString() + ".pb";

        List<String> acls = new ArrayList<>(docToStore.getOwnership().getAclsList());

        // Emit storage intent BEFORE upload — marks orphan candidates if upload/persist fails
        eventEmitter.emitStorageIntent(finalDocId, accountId, completeObjectKey,
                driveName, resolvedRequestId, connectorId, finalDatasourceId,
                docToStore.hasOwnership() ? docToStore.getOwnership() : null);

        // Branch on storage mode: S3-only (synchronous) vs Redis-buffered (async S3 flush)
        // In redis-buffered mode, if Redis is down, fall back to synchronous S3 write.
        boolean wantRedis = redisStorageConfig.resolvedStorageMode() == StorageMode.REDIS_BUFFERED;
        final boolean[] usedRedis = {false};

        Uni<?> storageWrite;
        if (wantRedis) {
            // Try Redis first; on failure, fall back to synchronous S3 write
            storageWrite = redisCache.put(nodeId.toString(), pipeDocBytes)
                    .invoke(() -> usedRedis[0] = true)
                    .onFailure().recoverWithUni(redisFail -> {
                        LOG.warnf(redisFail, "Redis PUT failed for nodeId=%s, falling back to synchronous S3 write", nodeId);
                        return Uni.createFrom().completionStage(
                                s3AsyncClient.putObject(PutObjectRequest.builder().bucket(resolvedBucket).key(completeObjectKey)
                                        .contentType(contentType).contentLength(sizeBytes).build(), AsyncRequestBody.fromBytes(pipeDocBytes))
                        ).replaceWithVoid();
                    });
        } else {
            // S3 path: synchronous write, wait for ACK
            storageWrite = Uni.createFrom().completionStage(
                    s3AsyncClient.putObject(PutObjectRequest.builder().bucket(resolvedBucket).key(completeObjectKey).contentType(contentType).contentLength(sizeBytes).build(), AsyncRequestBody.fromBytes(pipeDocBytes))
            );
        }

        return storageWrite
        .emitOn(runnable -> {
            if (requestContext != null) requestContext.runOnContext(v -> runnable.run());
            else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
        })
        .flatMap(writeResult -> {
            // Extract S3 metadata (only available when S3 was written directly)
            String s3VersionId = null;
            String s3Etag = null;
            if (!usedRedis[0] && writeResult instanceof software.amazon.awssdk.services.s3.model.PutObjectResponse putResponse) {
                s3VersionId = putResponse.versionId();
                s3Etag = putResponse.eTag();
            }
            final String finalVersionId = s3VersionId;
            final String finalEtag = s3Etag;
            final String pipeDocStatus = usedRedis[0] ? BackgroundS3Flusher.STATUS_PENDING_STORAGE : BackgroundS3Flusher.STATUS_AVAILABLE;

            final boolean[] isUpdate = {false};
            return Panache.<PipeDocRecord>withTransaction(() ->
                // 1. Upsert logical Document identity (retry on constraint violation for concurrent inserts)
                upsertDocument(finalDocId, accountId, finalDatasourceId, acls, contentType, sizeBytes,
                        checksum, usedRedis[0], nodeId, resolvedBucket, completeObjectKey, docToStore, clusterId)
                        .flatMap(ignored ->
                            // 2. Upsert PipeDocRecord (physical state) via native SQL ON CONFLICT DO UPDATE.
                            // This is atomic and handles concurrent inserts for the same node_id without
                            // the find-then-merge race condition that causes duplicate key violations
                            // during Kafka consumer rebalance re-deliveries.
                            PipeDocRecord.<PipeDocRecord>findById(nodeId)
                                    .invoke(existing -> { if (existing != null) isUpdate[0] = true; })
                                    .flatMap(existing -> {
                                        PipeDocRecord record = existing != null ? existing : new PipeDocRecord();
                                        if (existing == null) {
                                            record.nodeId = nodeId;
                                            record.docId = finalDocId;
                                            record.graphAddressId = resolvedGraphAddressId;
                                            record.createdAt = Instant.now();
                                        }
                                        record.clusterId = clusterId;
                                        record.accountId = accountId;
                                        record.datasourceId = finalDatasourceId;
                                        record.connectorId = connectorId;
                                        record.driveName = driveName;
                                        record.objectKey = completeObjectKey;
                                        record.pipedocObjectKey = completeObjectKey;
                                        record.versionId = finalVersionId;
                                        record.etag = finalEtag != null ? finalEtag : "";
                                        record.sizeBytes = sizeBytes;
                                        record.contentType = contentType;
                                        record.filename = nodeId.toString() + ".pb";
                                        record.checksum = checksum == null ? "" : checksum;
                                        record.acls = acls;
                                        record.status = pipeDocStatus;

                                        if (existing != null) {
                                            // Row exists — merge updates it
                                            return record.getSession().flatMap(session -> session.merge(record));
                                        }
                                        // Row doesn't exist — try persist, catch constraint violation, retry as merge
                                        return record.persist()
                                            .onFailure(DocumentStorageService::isConstraintViolation)
                                            .recoverWithUni(err -> {
                                                LOG.debugf("PipeDocRecord constraint violation for nodeId=%s, retrying as update", nodeId);
                                                return PipeDocRecord.<PipeDocRecord>findById(nodeId)
                                                    .flatMap(found -> {
                                                        if (found != null) {
                                                            found.clusterId = clusterId;
                                                            found.objectKey = completeObjectKey;
                                                            found.pipedocObjectKey = completeObjectKey;
                                                            found.versionId = finalVersionId;
                                                            found.etag = finalEtag != null ? finalEtag : "";
                                                            found.sizeBytes = sizeBytes;
                                                            found.contentType = contentType;
                                                            found.checksum = checksum == null ? "" : checksum;
                                                            found.status = pipeDocStatus;
                                                            return found.getSession().flatMap(s -> s.merge(found));
                                                        }
                                                        return Uni.createFrom().failure(err);
                                                    });
                                            });
                                    })
                                    .flatMap(v -> PipeDocRecord.<PipeDocRecord>findById(nodeId)))
            ).map(persisted -> {
                // Full Created event with catalog metadata — confirms upload + persist succeeded
                String docName = docToStore.hasSearchMetadata() && docToStore.getSearchMetadata().hasTitle()
                        ? docToStore.getSearchMetadata().getTitle() : finalDocId;
                String sourceMimeType = docToStore.hasSearchMetadata() && !docToStore.getSearchMetadata().getSourceMimeType().isEmpty()
                        ? docToStore.getSearchMetadata().getSourceMimeType() : contentType;
                eventEmitter.emitCreated(finalDocId, accountId, completeObjectKey, sizeBytes, checksum,
                        resolvedBucket, finalVersionId, finalEtag,
                        resolvedRequestId, connectorId, finalDatasourceId,
                        docToStore.hasOwnership() ? docToStore.getOwnership() : null,
                        docName, completeObjectKey, sourceMimeType);
                eventEmitter.emitPipeDocUpdate(isUpdate[0] ? "UPDATED" : "CREATED", nodeId.toString(), finalDocId, docToStore.hasSearchMetadata() ? docToStore.getSearchMetadata().getTitle() : null, null, docToStore.getOwnership(), DEFAULT_RETENTION_INTENT_DAYS);

                // In redis-buffered mode, emit a cache flush event for the background storage writer
                if (usedRedis[0]) {
                    try {
                        eventEmitter.emitCacheFlushEvent(nodeId.toString(), completeObjectKey, driveName, accountId);
                    } catch (Exception e) {
                        LOG.warnf(e, "Failed to emit cache flush event for nodeId=%s — document in Redis but flush may be delayed", nodeId);
                    }
                }

                return new StoredDocument(nodeId.toString(), completeObjectKey, finalVersionId, finalEtag, sizeBytes, checksum, persisted.createdAt.toEpochMilli());
            });
        });
    }

    public Uni<PipeDoc> get(String nodeId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        if (nodeId == null || nodeId.isBlank()) return Uni.createFrom().nullItem();
        UUID uuid;
        try { uuid = UUID.fromString(nodeId); } catch (IllegalArgumentException e) { return Uni.createFrom().nullItem(); }

        // Try Redis first (if redis-buffered mode), fall back to S3
        boolean useRedis = redisStorageConfig.resolvedStorageMode() == StorageMode.REDIS_BUFFERED;

        if (useRedis) {
            return redisCache.get(nodeId)
                    .onFailure().recoverWithNull()  // Redis down = cache miss, not failure
                    .flatMap(redisBytes -> {
                        if (redisBytes != null) {
                            try {
                                return Uni.createFrom().item(PipeDoc.parseFrom(redisBytes));
                            } catch (Exception e) {
                                LOG.warnf("Failed to parse PipeDoc from Redis for nodeId=%s, falling through to S3", nodeId);
                            }
                        }
                        // Cache miss or parse error — fall through to S3
                        return getFromS3(uuid, requestContext);
                    });
        }

        return getFromS3(uuid, requestContext);
    }

    private Uni<PipeDoc> getFromS3(UUID uuid, Context requestContext) {
        return PipeDocRecord.<PipeDocRecord>findById(uuid).flatMap(record -> {
            if (record == null) return Uni.createFrom().nullItem();
            return driveService.resolveDrive(record.driveName, record.accountId)
                    .flatMap(resolvedDrive -> Uni.createFrom().completionStage(
                            s3AsyncClient.getObject(
                                    GetObjectRequest.builder().bucket(resolvedDrive.bucket()).key(record.pipedocObjectKey).build(),
                                    AsyncResponseTransformer.toBytes())))
                    .emitOn(runnable -> {
                        if (requestContext != null) requestContext.runOnContext(v -> runnable.run());
                        else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                    })
                    .map(response -> {
                        try { return PipeDoc.parseFrom(response.asByteArray()); }
                        catch (Exception e) { return null; }
                    })
                    .onFailure(NoSuchKeyException.class).recoverWithItem((PipeDoc) null);
        });
    }

    /**
     * Deletes all {@link PipeDocRecord} rows for the logical document (and the {@link Document} catalog row),
     * optionally purging storage objects, then emits Kafka delete notifications.
     */
    public Uni<LogicalDeleteResult> deleteLogicalDocument(String docId, String accountId, String datasourceId, boolean purgeStorage) {
        if (docId == null || docId.isBlank() || accountId == null || accountId.isBlank() || datasourceId == null || datasourceId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("doc_id, account_id, and datasource_id are required"));
        }
        // Capture Vertx context — account validation gRPC callback may complete
        // on a non-Vertx thread, and Panache.withTransaction() requires it.
        io.vertx.core.Context callerContext = io.vertx.core.Vertx.currentContext();

        return accountCacheService.isValidAccount(accountId)
                .emitOn(runnable -> {
                    if (callerContext != null) callerContext.runOnContext(v -> runnable.run());
                    else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                })
                .flatMap(ok -> {
                    if (!ok) {
                        return Uni.createFrom().failure(new AccountValidationException("Account not found or inactive: " + accountId));
                    }
                    return Panache.withTransaction(() ->
                            PipeDocRecord.<PipeDocRecord>find("docId = ?1 and accountId = ?2 and datasourceId = ?3", docId, accountId, datasourceId).list()
                                    .flatMap(records -> {
                                        if (records.isEmpty()) {
                                            return Document.<Document>find("documentId = ?1 and accountId = ?2 and datasourceId = ?3", docId, accountId, datasourceId).firstResult()
                                                    .flatMap(doc -> {
                                                        if (doc == null) {
                                                            return Uni.createFrom().item(new DeleteTxnResult(List.of(), false));
                                                        }
                                                        return doc.delete().replaceWith(new DeleteTxnResult(List.of(), true));
                                                    });
                                        }
                                        List<DeletedPipeDocSnapshot> snapshots = records.stream().map(DeletedPipeDocSnapshot::from).toList();
                                        Uni<Void> del = Uni.createFrom().voidItem();
                                        for (PipeDocRecord r : records) {
                                            del = del.chain(() -> r.delete());
                                        }
                                        return del.flatMap(ignored -> Document.<Document>find("documentId = ?1 and accountId = ?2 and datasourceId = ?3", docId, accountId, datasourceId).firstResult()
                                                .flatMap(doc -> {
                                                    if (doc == null) {
                                                        return Uni.createFrom().item(new DeleteTxnResult(snapshots, false));
                                                    }
                                                    return doc.delete().replaceWith(new DeleteTxnResult(snapshots, false));
                                                }));
                                    })
                    ).flatMap(txn -> afterLogicalDelete(txn, docId, accountId, datasourceId, purgeStorage));
                });
    }

    private record DeleteTxnResult(List<DeletedPipeDocSnapshot> snapshots, boolean removedCatalogOnly) {}

    private Uni<LogicalDeleteResult> afterLogicalDelete(DeleteTxnResult txn, String docId, String accountId, String datasourceId, boolean purgeStorage) {
        if (txn.snapshots.isEmpty() && !txn.removedCatalogOnly) {
            return Uni.createFrom().item(new LogicalDeleteResult(0, List.of(), false));
        }
        // Capture Vertx context — S3 delete callbacks land on AWS Netty threads,
        // and we must return to Vertx context before the @WithSession interceptor
        // attempts to close the session.
        io.vertx.core.Context callerCtx = io.vertx.core.Vertx.currentContext();

        Uni<Void> purge = Uni.createFrom().voidItem();
        if (purgeStorage) {
            for (DeletedPipeDocSnapshot s : txn.snapshots) {
                purge = purge.chain(() -> deleteStorageObjects(s));
            }
        }
        List<UUID> nodeIds = txn.snapshots.stream().map(s -> s.nodeId).toList();
        int removedCount = txn.snapshots.size();
        return purge
                .emitOn(runnable -> {
                    if (callerCtx != null) callerCtx.runOnContext(v -> runnable.run());
                    else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                })
                .invoke(() -> {
                    for (DeletedPipeDocSnapshot s : txn.snapshots) {
                        OwnershipContext own = OwnershipContext.newBuilder()
                                .setAccountId(s.accountId)
                                .setDatasourceId(s.datasourceId)
                                .setConnectorId(s.connectorId != null ? s.connectorId : "")
                                .build();
                        eventEmitter.emitPipeDocUpdate("DELETED", s.nodeId.toString(), docId, null, null, own, DEFAULT_RETENTION_INTENT_DAYS);
                    }
                    String connectorId = txn.snapshots.isEmpty() ? null : txn.snapshots.get(0).connectorId;
                    eventEmitter.emitDeleted(docId, accountId, "deleteLogicalDocument", purgeStorage, UUID.randomUUID().toString(), connectorId, datasourceId);
                })
                .replaceWith(new LogicalDeleteResult(removedCount, nodeIds, txn.removedCatalogOnly));
    }

    private Uni<Void> deleteStorageObjects(DeletedPipeDocSnapshot s) {
        // Use cache-only drive resolution to avoid needing a Panache session —
        // this runs after the transaction closes, so no reactive session is available.
        String bucket = driveService.resolveBucketFromCache(s.driveName, s3Config.bucket());

        Uni<Void> u = deleteIfPresent(bucket, s.pipedocObjectKey);
        if (s.objectKey != null && !s.objectKey.isBlank() && !s.objectKey.equals(s.pipedocObjectKey)) {
            u = u.chain(() -> deleteIfPresent(bucket, s.objectKey));
        }
        return u.onFailure().invoke(e -> LOG.warnf(e, "Storage delete failed for node_id=%s", s.nodeId));
    }

    private Uni<Void> deleteIfPresent(String bucket, String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().completionStage(s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()))
                .replaceWithVoid()
                .onFailure().recoverWithUni(e -> {
                    LOG.warnf(e, "deleteObject failed bucket=%s key=%s", bucket, key);
                    return Uni.createFrom().voidItem();
                });
    }

    private record DeletedPipeDocSnapshot(UUID nodeId, String pipedocObjectKey, String objectKey, String driveName,
                                          String accountId, String datasourceId, String connectorId) {
        static DeletedPipeDocSnapshot from(PipeDocRecord r) {
            return new DeletedPipeDocSnapshot(r.nodeId, r.pipedocObjectKey, r.objectKey, r.driveName, r.accountId, r.datasourceId, r.connectorId);
        }
    }

    public Uni<PipeDoc> getByCompositeKey(String docId, String graphAddressId, String accountId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        if (docId == null || docId.isBlank() || graphAddressId == null || graphAddressId.isBlank() || accountId == null || accountId.isBlank()) return Uni.createFrom().nullItem();
        UUID nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        String nodeIdStr = nodeId.toString();

        // Try Redis first (if redis-buffered mode), fall back to S3
        boolean useRedis = redisStorageConfig.resolvedStorageMode() == StorageMode.REDIS_BUFFERED;
        if (useRedis) {
            return redisCache.get(nodeIdStr)
                    .onFailure().recoverWithNull()
                    .flatMap(redisBytes -> {
                        if (redisBytes != null) {
                            try {
                                return Uni.createFrom().item(PipeDoc.parseFrom(redisBytes));
                            } catch (Exception e) {
                                LOG.warnf("Failed to parse PipeDoc from Redis for nodeId=%s, falling through to S3", nodeIdStr);
                            }
                        }
                        return getFromS3(nodeId, requestContext);
                    });
        }
        return getFromS3(nodeId, requestContext);
    }

    private String buildObjectKeyBase(String keyPrefix, String driveName, String accountId, String connectorId, String datasourceId, String docId, String clusterId) {
        String basePath = String.join("/", sanitize(keyPrefix), sanitize(driveName), sanitize(accountId), sanitize(connectorId), sanitize(datasourceId), sanitize(docId));
        return (clusterId == null || clusterId.isBlank()) ? basePath + "/intake" : basePath + "/" + sanitize(clusterId);
    }

    private static String sanitize(String value) {
        if (value == null) return "unknown";
        String v = value.trim();
        return v.isEmpty() ? "unknown" : v.replace("/", "_").replace("\\", "_");
    }

    private static String computeDatasourceId(String accountId, String connectorId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest((accountId + ":" + (connectorId == null ? "" : connectorId)).getBytes(StandardCharsets.UTF_8))).substring(0, 16);
        } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    }

    /**
     * Upsert a Document entity. If concurrent inserts race and one hits a unique constraint
     * violation, retry by finding the now-existing record and updating it.
     */
    private Uni<Document> upsertDocument(String docId, String accountId, String datasourceId,
            List<String> acls, String contentType, long sizeBytes, String checksum,
            boolean usedRedis, UUID nodeId, String resolvedBucket, String completeObjectKey,
            PipeDoc docToStore, String clusterId) {

        return Document.<Document>find("documentId = ?1 and accountId = ?2 and datasourceId = ?3",
                        docId, accountId, datasourceId).firstResult()
                .flatMap(existingDoc -> {
                    Document doc = existingDoc != null ? existingDoc : new Document();
                    populateDocument(doc, docId, accountId, datasourceId, acls, contentType, sizeBytes,
                            checksum, usedRedis, nodeId, resolvedBucket, completeObjectKey, docToStore,
                            clusterId, existingDoc == null);

                    if (existingDoc == null) {
                        return doc.<Document>persist()
                                .onFailure(t -> isConstraintViolation(t)).recoverWithUni(t -> {
                                    LOG.debugf("Document insert race for doc_id=%s, retrying as update", docId);
                                    return Document.<Document>find("documentId = ?1 and accountId = ?2 and datasourceId = ?3",
                                                    docId, accountId, datasourceId).firstResult()
                                            .flatMap(retryDoc -> {
                                                if (retryDoc == null) {
                                                    return Uni.createFrom().failure(t);
                                                }
                                                populateDocument(retryDoc, docId, accountId, datasourceId, acls,
                                                        contentType, sizeBytes, checksum, usedRedis, nodeId,
                                                        resolvedBucket, completeObjectKey, docToStore, clusterId, false);
                                                return retryDoc.persist();
                                            });
                                });
                    }
                    return doc.persist();
                });
    }

    private void populateDocument(Document doc, String docId, String accountId, String datasourceId,
            List<String> acls, String contentType, long sizeBytes, String checksum,
            boolean usedRedis, UUID nodeId, String resolvedBucket, String completeObjectKey,
            PipeDoc docToStore, String clusterId, boolean isNew) {
        doc.documentId = docId;
        doc.accountId = accountId;
        doc.datasourceId = datasourceId;
        doc.acls = acls;
        doc.contentType = contentType;
        doc.contentSize = sizeBytes;
        doc.checksum = checksum;
        doc.storageLocation = usedRedis
                ? "redis://pipedoc:" + nodeId
                : "s3://" + resolvedBucket + "/" + completeObjectKey;
        doc.updatedAt = Instant.now();
        doc.title = docToStore.hasSearchMetadata() && docToStore.getSearchMetadata().hasTitle()
                ? docToStore.getSearchMetadata().getTitle() : docId;

        String filename = extractFilename(docToStore);
        doc.filename = (filename != null && !filename.isBlank()) ? filename : doc.title;

        if (isNew) {
            doc.createdAt = Instant.now();
            doc.version = 1;
            doc.status = clusterId == null ? "INTAKE" : "PROCESSING";
        } else {
            doc.version++;
            if (clusterId != null) doc.status = "PROCESSING";
        }
    }

    /**
     * Extracts the best filename from a PipeDoc's blob bag.
     * Checks single blob first, then iterates all blobs for the first non-empty filename.
     */
    private static String extractFilename(PipeDoc doc) {
        if (!doc.hasBlobBag()) return null;
        BlobBag bag = doc.getBlobBag();
        if (bag.hasBlob() && !bag.getBlob().getFilename().isEmpty()) {
            return bag.getBlob().getFilename();
        }
        if (bag.hasBlobs()) {
            for (Blob blob : bag.getBlobs().getBlobList()) {
                if (!blob.getFilename().isEmpty()) return blob.getFilename();
            }
        }
        return null;
    }

    private static boolean isConstraintViolation(Throwable t) {
        if (t instanceof org.hibernate.exception.ConstraintViolationException) return true;
        String msg = t.getMessage();
        return msg != null && (msg.contains("23505") || msg.contains("duplicate key") || msg.contains("unique constraint"));
    }

    private static String computeSha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) { return ""; }
    }

    public record StoredDocument(String documentId, String s3Key, String versionId, String etag, long sizeBytes, String checksum, long createdAtEpochMs) {}

    public Uni<java.util.List<PipeDocRecord>> findDocumentById(String docId) {
        if (docId == null || docId.isBlank()) return Uni.createFrom().failure(new DocumentNotFoundException("Document ID required"));
        return PipeDocRecord.<PipeDocRecord>find("docId", docId).list().onFailure().transform(throwable -> new DocumentQueryException("Query failed", throwable));
    }

    public Uni<DocumentSearchResult> findDocumentsByCriteria(DocumentSearchCriteria criteria) {
        if (criteria == null) return Uni.createFrom().failure(new IllegalArgumentException("Criteria required"));
        StringBuilder qb = new StringBuilder(); List<Object> params = new ArrayList<>(); int pi = 1;
        if (isValid(criteria.datasourceId())) { qb.append("datasourceId = ?").append(pi++); params.add(criteria.datasourceId()); }
        if (isValid(criteria.accountId())) { if (!qb.isEmpty()) qb.append(" and "); qb.append("accountId = ?").append(pi++); params.add(criteria.accountId()); }
        if (isValid(criteria.connectorId())) { if (!qb.isEmpty()) qb.append(" and "); qb.append("connectorId = ?").append(pi++); params.add(criteria.connectorId()); }
        if (criteria.clusterId() != null) { if (!qb.isEmpty()) qb.append(" and "); if (criteria.clusterId().isBlank()) qb.append("clusterId is null"); else { qb.append("clusterId = ?").append(pi++); params.add(criteria.clusterId()); } }
        if (criteria.createdAfter() != null) { if (!qb.isEmpty()) qb.append(" and "); qb.append("createdAt >= ?").append(pi++); params.add(criteria.createdAfter()); }
        if (criteria.createdBefore() != null) { if (!qb.isEmpty()) qb.append(" and "); qb.append("createdAt <= ?").append(pi++); params.add(criteria.createdBefore()); }
        String query = !qb.isEmpty() ? qb.toString() : DEFAULT_QUERY_CONDITION;
        return Uni.combine().all().unis(PipeDocRecord.count(query, params.toArray()), PipeDocRecord.<PipeDocRecord>find(query + DEFAULT_ORDER_BY, params.toArray()).page(criteria.page() - 1, criteria.pageSize()).list()).asTuple()
                .map(t -> new DocumentSearchResult(t.getItem2(), t.getItem1(), criteria.page(), criteria.pageSize(), (int) Math.ceil((double) t.getItem1() / criteria.pageSize())))
                .onFailure().transform(throwable -> new DocumentQueryException("Query failed", throwable));
    }

    private boolean isValid(String v) { return v != null && !v.isBlank(); }

    public record DocumentSearchCriteria(String datasourceId, String accountId, String connectorId, String clusterId, Instant createdAfter, Instant createdBefore, int page, int pageSize) {
        public DocumentSearchCriteria {
            if (page < 1 || pageSize < 1 || pageSize > MAX_PAGE_SIZE) throw new IllegalArgumentException("Invalid pagination");
        }
    }

    public record DocumentSearchResult(java.util.List<PipeDocRecord> documents, long totalCount, int currentPage, int pageSize, int totalPages) {}
    public static class DocumentNotFoundException extends RuntimeException { public DocumentNotFoundException(String m) { super(m); } }
    public static class DocumentQueryException extends RuntimeException { public DocumentQueryException(String m, Throwable c) { super(m, c); } }
    public static class AccountValidationException extends RuntimeException { public AccountValidationException(String m) { super(m); } }
}
