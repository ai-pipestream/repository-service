package ai.pipestream.repository.service;

import ai.pipestream.data.v1.Blob;
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
 */
@ApplicationScoped
public class DocumentStorageService {

    private static final Logger LOG = Logger.getLogger(DocumentStorageService.class);
    private static final int MAX_PAGE_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final String DEFAULT_QUERY_CONDITION = "1=1";
    private static final String DEFAULT_ORDER_BY = " order by createdAt desc";

    @Inject
    S3AsyncClient s3AsyncClient;

    @Inject
    S3Config s3Config;

    @Inject
    AccountCacheService accountCacheService;

    @Inject
    RepositoryEventEmitter eventEmitter;

    @Inject
    io.vertx.mutiny.core.Vertx vertx;

    @Inject
    PipeDocUuidGenerator uuidGenerator;

    @Inject
    DriveService driveService;

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
        if (document == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("document must not be null"));
        }

        if (!document.hasOwnership()) {
             return Uni.createFrom().failure(new IllegalArgumentException("document must have ownership context"));
        }
        OwnershipContext ownership = document.getOwnership();
        String accountId = ownership.getAccountId();
        String connectorId = ownership.getConnectorId();
        String datasourceId = ownership.getDatasourceId();

        if (accountId == null || accountId.isBlank()) {
             return Uni.createFrom().failure(new IllegalArgumentException("ownership.account_id is required"));
        }

        return accountCacheService.isValidAccount(accountId)
                .flatMap(isValid -> {
                    if (!isValid) {
                        return Uni.createFrom().failure(new AccountValidationException("Account not found or inactive: " + accountId));
                    }

                    String documentId = document.getDocId();
                    PipeDoc docToStore;
                    if (documentId == null || documentId.isBlank()) {
                        documentId = UUID.randomUUID().toString();
                        docToStore = document.toBuilder().setDocId(documentId).build();
                    } else {
                        docToStore = document;
                    }
                    final String finalDocId = documentId;

                    String finalDatasourceId;
                    if (datasourceId == null || datasourceId.isBlank()) {
                        finalDatasourceId = computeDatasourceId(accountId, connectorId);
                    } else {
                        finalDatasourceId = datasourceId;
                    }

                    String resolvedRequestId = (requestId == null || requestId.isBlank())
                            ? UUID.randomUUID().toString()
                            : requestId;

                    String finalGraphAddressId = (graphLocationId != null && !graphLocationId.isBlank()) 
                            ? graphLocationId 
                            : finalDatasourceId;

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

        LOG.infof("Storing PipeDoc doc_id=%s, graph_address_id=%s, cluster_id=%s to s3://%s/%s (bytes=%d)",
                finalDocId, resolvedGraphAddressId, clusterId != null ? clusterId : "intake",
                resolvedBucket, completeObjectKey, sizeBytes);

        List<String> acls = new ArrayList<>(docToStore.getOwnership().getAclsList());

        return Uni.createFrom().completionStage(
                s3AsyncClient.putObject(
                        PutObjectRequest.builder()
                                .bucket(resolvedBucket)
                                .key(completeObjectKey)
                                .contentType(contentType)
                                .contentLength(sizeBytes)
                                .build(),
                        AsyncRequestBody.fromBytes(pipeDocBytes)
                )
        )
        .emitOn(runnable -> {
            if (requestContext != null) {
                requestContext.runOnContext(v -> runnable.run());
            } else {
                vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
            }
        })
        .flatMap(putResponse -> {
            String etag = putResponse.eTag();
            String versionId = putResponse.versionId();

            final boolean[] isUpdate = {false};
            return Panache.withTransaction(() -> {
                // 1. Upsert logical Document identity
                Uni<Document> docUpsert = Document.<Document>find("documentId", finalDocId).firstResult()
                        .flatMap(existingDoc -> {
                            Document doc = existingDoc != null ? existingDoc : new Document();
                            doc.documentId = finalDocId;
                            doc.accountId = accountId;
                            doc.datasourceId = finalDatasourceId;
                            doc.acls = acls;
                            doc.contentType = contentType;
                            doc.contentSize = sizeBytes;
                            doc.checksum = checksum;
                            doc.storageLocation = "s3://" + resolvedBucket + "/" + completeObjectKey;
                            doc.updatedAt = Instant.now();
                            
                            // Extract title and filename
                            doc.title = docToStore.hasSearchMetadata() && docToStore.getSearchMetadata().hasTitle() 
                                    ? docToStore.getSearchMetadata().getTitle() : finalDocId;
                            
                            String filename = null;
                            if (docToStore.hasBlobBag()) {
                                if (docToStore.getBlobBag().hasBlob()) {
                                    filename = docToStore.getBlobBag().getBlob().getFilename();
                                } else if (docToStore.getBlobBag().hasBlobs() && docToStore.getBlobBag().getBlobs().getBlobCount() > 0) {
                                    filename = docToStore.getBlobBag().getBlobs().getBlob(0).getFilename();
                                }
                            }
                            doc.filename = (filename != null && !filename.isBlank()) ? filename : doc.title;
                            
                            if (existingDoc == null) {
                                doc.createdAt = Instant.now();
                                doc.version = 1;
                                doc.status = clusterId == null ? "INTAKE" : "PROCESSING";
                                return doc.persist();
                            } else {
                                doc.version++;
                                if (clusterId != null) doc.status = "PROCESSING";
                                return doc.persist();
                            }
                        });

                // 2. Upsert PipeDocRecord (physical state)
                Uni<PipeDocRecord> recordUpsert = PipeDocRecord.<PipeDocRecord>findById(nodeId)
                        .flatMap(existingRecord -> {
                            PipeDocRecord record;
                            if (existingRecord != null) {
                                isUpdate[0] = true;
                                record = existingRecord;
                            } else {
                                record = new PipeDocRecord();
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
                            record.versionId = versionId;
                            record.etag = etag;
                            record.sizeBytes = sizeBytes;
                            record.contentType = contentType;
                            record.filename = nodeId.toString() + ".pb";
                            record.checksum = checksum == null ? "" : checksum;
                            record.acls = acls;
                            return record.persist();
                        });

                return Uni.combine().all().unis(docUpsert, recordUpsert).asTuple();
            }).map(tuple -> {
                PipeDocRecord persisted = tuple.getItem2();
                // 6. Emit Events
                eventEmitter.emitCreated(finalDocId, accountId, completeObjectKey, completeObjectKey, sizeBytes, checksum, resolvedBucket, versionId, resolvedRequestId, connectorId, finalDatasourceId);
                eventEmitter.emitPipeDocUpdate(isUpdate[0] ? "UPDATED" : "CREATED", nodeId.toString(), finalDocId, null, null);

                return new StoredDocument(
                        nodeId.toString(), 
                        completeObjectKey, 
                        versionId, 
                        etag, 
                        sizeBytes, 
                        checksum,
                        persisted.createdAt != null ? persisted.createdAt.toEpochMilli() : Instant.now().toEpochMilli()
                );
            });
        });
    }

    public Uni<PipeDoc> get(String nodeId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        if (nodeId == null || nodeId.isBlank()) return Uni.createFrom().nullItem();
        UUID uuid;
        try {
            uuid = UUID.fromString(nodeId);
        } catch (IllegalArgumentException e) {
            return Uni.createFrom().nullItem();
        }
        return PipeDocRecord.<PipeDocRecord>findById(uuid)
                .flatMap(record -> {
                    if (record == null) return Uni.createFrom().nullItem();
                    return driveService.resolveDrive(record.driveName, record.accountId)
                            .flatMap(resolvedDrive -> Uni.createFrom().completionStage(
                                    s3AsyncClient.getObject(GetObjectRequest.builder().bucket(resolvedDrive.bucket()).key(record.pipedocObjectKey).build(), AsyncResponseTransformer.toBytes())
                            ))
                            .emitOn(runnable -> {
                                if (requestContext != null) requestContext.runOnContext(v -> runnable.run());
                                else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                            })
                            .map(response -> {
                                try {
                                    return PipeDoc.parseFrom(response.asByteArray());
                                } catch (Exception e) {
                                    LOG.errorf(e, "Failed to parse PipeDoc from S3 for node_id=%s", nodeId);
                                    return null;
                                }
                            }).onFailure(NoSuchKeyException.class).recoverWithItem((PipeDoc) null);
                });
    }

    public Uni<PipeDoc> getByCompositeKey(String docId, String graphAddressId, String accountId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        if (docId == null || docId.isBlank() || graphAddressId == null || graphAddressId.isBlank() || accountId == null || accountId.isBlank()) {
            return Uni.createFrom().nullItem();
        }
        UUID nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        return PipeDocRecord.<PipeDocRecord>findById(nodeId)
                .flatMap(record -> {
                    if (record == null) return Uni.createFrom().nullItem();
                    return driveService.resolveDrive(record.driveName, record.accountId)
                            .flatMap(resolvedDrive -> Uni.createFrom().completionStage(
                                    s3AsyncClient.getObject(GetObjectRequest.builder().bucket(resolvedDrive.bucket()).key(record.pipedocObjectKey).build(), AsyncResponseTransformer.toBytes())
                            ))
                            .emitOn(runnable -> {
                                if (requestContext != null) requestContext.runOnContext(v -> runnable.run());
                                else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                            })
                            .map(response -> {
                                try {
                                    return PipeDoc.parseFrom(response.asByteArray());
                                } catch (Exception e) {
                                    LOG.errorf(e, "Failed to parse PipeDoc from S3 for doc_id=%s, graph_address_id=%s, account_id=%s", docId, graphAddressId, accountId);
                                    return null;
                                }
                            }).onFailure(NoSuchKeyException.class).recoverWithItem((PipeDoc) null);
                });
    }

    private String buildObjectKeyBase(String keyPrefix, String driveName, String accountId, String connectorId, String datasourceId, String docId, String clusterId) {
        String basePath = String.join("/", sanitize(keyPrefix), sanitize(driveName), sanitize(accountId), sanitize(connectorId), sanitize(datasourceId), sanitize(docId));
        return (clusterId == null || clusterId.isBlank()) ? basePath + "/intake" : basePath + "/" + sanitize(clusterId);
    }

    private static String sanitize(String value) {
        if (value == null) return "unknown";
        String v = value.trim();
        if (v.isEmpty()) return "unknown";
        return v.replace("/", "_").replace("\\", "_");
    }

    private static String computeDatasourceId(String accountId, String connectorId) {
        String input = accountId + ":" + (connectorId == null ? "" : connectorId);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static String computeSha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    public record StoredDocument(String documentId, String s3Key, String versionId, String etag, long sizeBytes, String checksum, long createdAtEpochMs) {}

    public Uni<java.util.List<PipeDocRecord>> findDocumentById(String docId) {
        if (docId == null || docId.isBlank()) return Uni.createFrom().failure(new DocumentNotFoundException("Document ID must not be null or blank"));
        return PipeDocRecord.<PipeDocRecord>find("docId", docId).list()
                .onFailure().transform(throwable -> new DocumentQueryException("Failed to query documents by ID", throwable));
    }

    public Uni<DocumentSearchResult> findDocumentsByCriteria(DocumentSearchCriteria criteria) {
        if (criteria == null) return Uni.createFrom().failure(new IllegalArgumentException("Search criteria must not be null"));
        StringBuilder queryBuilder = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        int paramIndex = 1;
        if (isValidFilterValue(criteria.datasourceId())) { queryBuilder.append("datasourceId = ?").append(paramIndex++); params.add(criteria.datasourceId()); }
        if (isValidFilterValue(criteria.accountId())) { if (!queryBuilder.isEmpty()) queryBuilder.append(" and "); queryBuilder.append("accountId = ?").append(paramIndex++); params.add(criteria.accountId()); }
        if (isValidFilterValue(criteria.connectorId())) { if (!queryBuilder.isEmpty()) queryBuilder.append(" and "); queryBuilder.append("connectorId = ?").append(paramIndex++); params.add(criteria.connectorId()); }
        if (criteria.clusterId() != null) { if (!queryBuilder.isEmpty()) queryBuilder.append(" and "); if (criteria.clusterId().isBlank()) queryBuilder.append("clusterId is null"); else { queryBuilder.append("clusterId = ?").append(paramIndex++); params.add(criteria.clusterId()); } }
        if (criteria.createdAfter() != null) { if (!queryBuilder.isEmpty()) queryBuilder.append(" and "); queryBuilder.append("createdAt >= ?").append(paramIndex++); params.add(criteria.createdAfter()); }
        if (criteria.createdBefore() != null) { if (!queryBuilder.isEmpty()) queryBuilder.append(" and "); queryBuilder.append("createdAt <= ?").append(paramIndex++); params.add(criteria.createdBefore()); }
        String query = !queryBuilder.isEmpty() ? queryBuilder.toString() : DEFAULT_QUERY_CONDITION;
        Uni<Long> countUni = PipeDocRecord.count(query, params.toArray());
        int pageSize = criteria.pageSize();
        int page = criteria.page();
        Uni<java.util.List<PipeDocRecord>> resultsUni = PipeDocRecord.<PipeDocRecord>find(query + DEFAULT_ORDER_BY, params.toArray()).page(page - 1, pageSize).list();
        return Uni.combine().all().unis(countUni, resultsUni).asTuple().map(tuple -> new DocumentSearchResult(tuple.getItem2(), tuple.getItem1(), page, pageSize, (int) Math.ceil((double) tuple.getItem1() / pageSize)))
                .onFailure().transform(throwable -> new DocumentQueryException("Failed to query documents by criteria", throwable));
    }

    private boolean isValidFilterValue(String value) { return value != null && !value.isBlank(); }

    public record DocumentSearchCriteria(String datasourceId, String accountId, String connectorId, String clusterId, Instant createdAfter, Instant createdBefore, int page, int pageSize) {
        public DocumentSearchCriteria {
            if (page < 1) throw new IllegalArgumentException("Page must be >= 1");
            if (pageSize < 1) throw new IllegalArgumentException("Page size must be >= 1");
            if (pageSize > MAX_PAGE_SIZE) throw new IllegalArgumentException("Page size must be <= " + MAX_PAGE_SIZE);
        }
    }

    public record DocumentSearchResult(java.util.List<PipeDocRecord> documents, long totalCount, int currentPage, int pageSize, int totalPages) {}
    public static class DocumentNotFoundException extends RuntimeException { public DocumentNotFoundException(String message) { super(message); } }
    public static class DocumentQueryException extends RuntimeException { public DocumentQueryException(String message, Throwable cause) { super(message, cause); } }
    public static class AccountValidationException extends RuntimeException { public AccountValidationException(String message) { super(message); } }
}
