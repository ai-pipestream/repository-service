package ai.pipestream.repository.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.account.AccountCacheService;
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
import java.util.HexFormat;
import java.util.UUID;

/**
 * Service for managing document storage operations.
 * Handles CRUD operations for documents in the repository.
 *
 * For gRPC path: validates account, stores PipeDoc to S3, persists metadata, emits events.
 * Fully Reactive Implementation.
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

    public DocumentStorageService() {
        LOG.info("DocumentStorageService initialized (Reactive)");
    }

    /**
     * Store a {@link PipeDoc}.
     *
     * Validates account, stores PipeDoc to S3, persists metadata to DB, emits event.
     * Uses datasource_id from ownership as the graph_address_id (for initial intake).
     *
     * @param document the document to store
     * @return Uni containing storage result containing the resolved document id and S3 key
     */
    public Uni<StoredDocument> store(PipeDoc document) {
        return store(document, null, null, null);
    }

    /**
     * Store a {@link PipeDoc} with a specific request ID for tracing.
     *
     * Uses datasource_id from ownership as the graph_address_id (for initial intake).
     *
     * @param document the document to store
     * @param requestId optional request ID for tracing (auto-generated if null)
     * @return Uni containing storage result
     */
    public Uni<StoredDocument> store(PipeDoc document, String requestId) {
        return store(document, requestId, null, null);
    }

    /**
     * Store a {@link PipeDoc} with a specific request ID, graph location ID, and cluster ID.
     *
     * @param document the document to store
     * @param requestId optional request ID for tracing (auto-generated if null)
     * @param graphLocationId optional graph location ID (if null, uses datasource_id from ownership)
     * @param clusterId optional cluster ID (null for intake, set for cluster processing)
     * @return Uni containing storage result
     */
    public Uni<StoredDocument> store(PipeDoc document, String requestId, String graphLocationId, String clusterId) {
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

        // 1. Validate Account
        return accountCacheService.isValidAccount(accountId)
                .flatMap(isValid -> {
                    if (!isValid) {
                        return Uni.createFrom().failure(new AccountValidationException("Account not found or inactive: " + accountId));
                    }

                    // Prepare metadata
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

                    // Determine graph_address_id: use provided graphLocationId, or fallback to datasource_id
                    // Note: We do not validate graph_location_id here. Repository Service is "dumb storage".
                    // Validation of topology happens downstream in the Engine when processing events.
                    String finalGraphAddressId = (graphLocationId != null && !graphLocationId.isBlank()) 
                            ? graphLocationId 
                            : finalDatasourceId;

                    return continueStoring(docToStore, finalDocId, accountId, connectorId, finalDatasourceId,
                            resolvedRequestId, finalGraphAddressId, clusterId);
                });
    }

    /**
     * Continues the storage process after graph address ID is resolved.
     */
    private Uni<StoredDocument> continueStoring(PipeDoc docToStore, String finalDocId, String accountId, 
            String connectorId, String finalDatasourceId, String resolvedRequestId, String resolvedGraphAddressId, String clusterId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        
        String driveName = "default";
        
        byte[] pipeDocBytes = docToStore.toByteArray();
        String contentType = "application/x-protobuf";
        long sizeBytes = pipeDocBytes.length;
        String checksum = computeSha256(pipeDocBytes);

        // 2. Generate deterministic UUID for node_id using (doc_id, graph_address_id, account_id)
        UUID nodeId = uuidGenerator.generateNodeId(finalDocId, resolvedGraphAddressId, accountId);
        
        // 3. Build complete S3 object key with UUID filename
        String basePath = buildObjectKeyBase(driveName, accountId, connectorId, finalDatasourceId, finalDocId, clusterId);
        String completeObjectKey = basePath + "/" + nodeId.toString() + ".pb";
        
        LOG.infof("Storing PipeDoc doc_id=%s, graph_address_id=%s, cluster_id=%s to s3://%s/%s (bytes=%d)",
                finalDocId, resolvedGraphAddressId, clusterId != null ? clusterId : "intake", 
                s3Config.bucket(), completeObjectKey, sizeBytes);

        // 4. Store to S3
        return Uni.createFrom().completionStage(
                s3AsyncClient.putObject(
                        PutObjectRequest.builder()
                                .bucket(s3Config.bucket())
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
                // Fallback if no context (e.g. background task), though unlikely in gRPC
                vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
            }
        })
        .flatMap(putResponse -> {
            String etag = putResponse.eTag();
            String versionId = putResponse.versionId();

            // 5. Persist to DB (Reactive Transaction with UPSERT pattern)
            return Panache.<PipeDocRecord>withTransaction(() -> 
                PipeDocRecord.<PipeDocRecord>findById(nodeId)
                        .flatMap(existingRecord -> {
                            if (existingRecord != null) {
                                // UPDATE existing record (new version)
                                LOG.debugf("Updating existing PipeDocRecord: node_id=%s", nodeId);
                                existingRecord.objectKey = completeObjectKey;
                                existingRecord.pipedocObjectKey = completeObjectKey;
                                existingRecord.clusterId = clusterId; // Update cluster_id if changed
                                existingRecord.versionId = versionId;
                                existingRecord.etag = etag;
                                existingRecord.sizeBytes = sizeBytes;
                                existingRecord.contentType = contentType;
                                existingRecord.checksum = checksum == null ? "" : checksum;
                                // Note: created_at stays the same, we could add updated_at if needed
                                return existingRecord.persist();
                            } else {
                                // INSERT new record
                                LOG.debugf("Creating new PipeDocRecord: node_id=%s", nodeId);
                                PipeDocRecord record = new PipeDocRecord();
                                record.nodeId = nodeId;
                                record.docId = finalDocId;
                                record.graphAddressId = resolvedGraphAddressId;
                                record.clusterId = clusterId; // NULL for intake, cluster name for processing
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
                                record.createdAt = Instant.now();
                                return record.persist();
                            }
                        })
            ).map((PipeDocRecord persisted) -> {
                // 6. Emit Event
                eventEmitter.emitCreated(
                        finalDocId,
                        accountId,
                        completeObjectKey,
                        completeObjectKey,
                        sizeBytes,
                        checksum,
                        s3Config.bucket(),
                        versionId,
                        resolvedRequestId,
                        connectorId,
                        finalDatasourceId
                );

                // Return node_id (UUID) as the repository identifier
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

    /**
     * Retrieve a PipeDoc by repository node ID (UUID).
     *
     * @param nodeId the repository node ID (UUID as string)
     * @return Uni containing the PipeDoc if found, or null/empty
     */
    public Uni<PipeDoc> get(String nodeId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        
        if (nodeId == null || nodeId.isBlank()) {
            return Uni.createFrom().nullItem();
        }

        UUID uuid;
        try {
            uuid = UUID.fromString(nodeId);
        } catch (IllegalArgumentException e) {
            LOG.warnf("Invalid UUID format for node_id: %s", nodeId);
            return Uni.createFrom().nullItem();
        }

        // Look up metadata in DB by node_id (PK)
        return PipeDocRecord.<PipeDocRecord>findById(uuid)
                .flatMap(record -> {
                    if (record == null) {
                        return Uni.createFrom().nullItem();
                    }

                    // Fetch from S3
                    return Uni.createFrom().completionStage(
                            s3AsyncClient.getObject(
                                    GetObjectRequest.builder()
                                            .bucket(s3Config.bucket())
                                            .key(record.pipedocObjectKey)
                                            .build(),
                                    AsyncResponseTransformer.toBytes()
                            )
                    )
                    .emitOn(runnable -> {
                        if (requestContext != null) {
                            requestContext.runOnContext(v -> runnable.run());
                        } else {
                            vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                        }
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

    /**
     * Retrieve a PipeDoc by logical identifiers (for initial intake lookups).
     * Uses the composite key (doc_id, graph_address_id, account_id) to find the document.
     * For initial intake, graph_address_id is typically the datasource_id.
     *
     * @param docId The document identifier
     * @param graphAddressId The graph address ID (datasource_id for intake, or graph node ID)
     * @param accountId The account identifier
     * @return Uni containing the PipeDoc if found, or null/empty
     */
    public Uni<PipeDoc> getByCompositeKey(String docId, String graphAddressId, String accountId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        
        if (docId == null || docId.isBlank() || graphAddressId == null || graphAddressId.isBlank() 
                || accountId == null || accountId.isBlank()) {
            return Uni.createFrom().nullItem();
        }

        // Generate the deterministic UUID to look up by PK
        UUID nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);

        // Look up metadata in DB by node_id (PK)
        return PipeDocRecord.<PipeDocRecord>findById(nodeId)
                .flatMap(record -> {
                    if (record == null) {
                        return Uni.createFrom().nullItem();
                    }

                    // Fetch from S3
                    return Uni.createFrom().completionStage(
                            s3AsyncClient.getObject(
                                    GetObjectRequest.builder()
                                            .bucket(s3Config.bucket())
                                            .key(record.pipedocObjectKey)
                                            .build(),
                                    AsyncResponseTransformer.toBytes()
                            )
                    )
                    .emitOn(runnable -> {
                        if (requestContext != null) {
                            requestContext.runOnContext(v -> runnable.run());
                        } else {
                            vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                        }
                    })
                    .map(response -> {
                        try {
                            return PipeDoc.parseFrom(response.asByteArray());
                        } catch (Exception e) {
                            LOG.errorf(e, "Failed to parse PipeDoc from S3 for doc_id=%s, graph_address_id=%s, account_id=%s", 
                                    docId, graphAddressId, accountId);
                            return null;
                        }
                    }).onFailure(NoSuchKeyException.class).recoverWithItem((PipeDoc) null);
                });
    }


    /**
     * Builds the S3 object key base path for a PipeDoc (without UUID filename).
     * 
     * Path structure:
     * - Intake: {prefix}/{account}/{connector}/{datasource}/{docId}/intake
     * - Cluster: {prefix}/{account}/{connector}/{datasource}/{docId}/{clusterId}
     * 
     * The UUID filename will be appended by the caller: basePath + "/" + uuid + ".pb"
     */
    private String buildObjectKeyBase(String driveName, String accountId, String connectorId, String datasourceId, String docId, String clusterId) {
        String safeDrive = sanitize(driveName);
        String safeAccount = sanitize(accountId);
        String safeConnector = sanitize(connectorId);
        String safeDatasource = sanitize(datasourceId);
        String safeDoc = sanitize(docId);
        
        String basePath = String.join("/",
                sanitize(s3Config.keyPrefix()),
                safeDrive,
                safeAccount,
                safeConnector,
                safeDatasource,
                safeDoc
        );
        
        // Add subdirectory: "intake" for intake, clusterId for cluster processing
        if (clusterId == null || clusterId.isBlank()) {
            return basePath + "/intake";
        } else {
            return basePath + "/" + sanitize(clusterId);
        }
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

    /**
     * Result of storing a document.
     */
    public record StoredDocument(
            String documentId,
            String s3Key,
            String versionId,
            String etag,
            long sizeBytes,
            String checksum,
            long createdAtEpochMs
    ) {}

    /**
     * Find a document by its document ID (doc_id).
     * Returns all PipeDocRecords with the given doc_id.
     *
     * @param docId the document ID to search for
     * @return Uni containing list of PipeDocRecords with the given doc_id
     */
    public Uni<java.util.List<PipeDocRecord>> findDocumentById(String docId) {
        if (docId == null || docId.isBlank()) {
            return Uni.createFrom().failure(new DocumentNotFoundException("Document ID must not be null or blank"));
        }

        LOG.debugf("Finding documents by doc_id: %s", docId);

        return PipeDocRecord.<PipeDocRecord>find("docId", docId).list()
                .onFailure().transform(throwable -> {
                    LOG.errorf(throwable, "Failed to find documents by doc_id: %s", docId);
                    return new DocumentQueryException("Failed to query documents by ID", throwable);
                });
    }

    /**
     * Find documents by various criteria with pagination support.
     *
     * @param criteria the search criteria
     * @return Uni containing paginated list of PipeDocRecords matching the criteria
     */
    public Uni<DocumentSearchResult> findDocumentsByCriteria(DocumentSearchCriteria criteria) {
        if (criteria == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("Search criteria must not be null"));
        }

        LOG.debugf("Finding documents by criteria: %s", criteria);

        // Build query dynamically based on criteria
        StringBuilder queryBuilder = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        int paramIndex = 1;

        // Add datasourceId filter (source)
        if (isValidFilterValue(criteria.datasourceId())) {
            queryBuilder.append("datasourceId = ?").append(paramIndex++);
            params.add(criteria.datasourceId());
        }

        // Add accountId filter
        if (isValidFilterValue(criteria.accountId())) {
            if (!queryBuilder.isEmpty()) {
                queryBuilder.append(" and ");
            }
            queryBuilder.append("accountId = ?").append(paramIndex++);
            params.add(criteria.accountId());
        }

        // Add connectorId filter
        if (isValidFilterValue(criteria.connectorId())) {
            if (!queryBuilder.isEmpty()) {
                queryBuilder.append(" and ");
            }
            queryBuilder.append("connectorId = ?").append(paramIndex++);
            params.add(criteria.connectorId());
        }

        // Add clusterId filter (can be used as a proxy for status: null=intake, value=processed)
        if (criteria.clusterId() != null) {
            if (!queryBuilder.isEmpty()) {
                queryBuilder.append(" and ");
            }
            if (criteria.clusterId().isBlank()) {
                // Empty string means filter for null cluster_id (intake documents)
                queryBuilder.append("clusterId is null");
            } else {
                queryBuilder.append("clusterId = ?").append(paramIndex++);
                params.add(criteria.clusterId());
            }
        }

        // Add date range filters
        if (criteria.createdAfter() != null) {
            if (!queryBuilder.isEmpty()) {
                queryBuilder.append(" and ");
            }
            queryBuilder.append("createdAt >= ?").append(paramIndex++);
            params.add(criteria.createdAfter());
        }

        if (criteria.createdBefore() != null) {
            if (!queryBuilder.isEmpty()) {
                queryBuilder.append(" and ");
            }
            queryBuilder.append("createdAt <= ?").append(paramIndex++);
            params.add(criteria.createdBefore());
        }

        // Default to selecting all if no criteria provided
        boolean hasFilters = !queryBuilder.isEmpty();
        String query = hasFilters ? queryBuilder.toString() : DEFAULT_QUERY_CONDITION;
        
        // Log warning if no filters provided to avoid accidental full table scan
        if (!hasFilters) {
            LOG.warnf("findDocumentsByCriteria called with no filter criteria - this will return all documents");
        }
        
        // Add ordering
        String orderBy = DEFAULT_ORDER_BY;

        LOG.debugf("Executing query: %s with %d parameters", query, params.size());

        // Count total matching records
        Uni<Long> countUni = PipeDocRecord.count(query, params.toArray());

        // Fetch paginated results
        int pageSize = criteria.pageSize();
        int page = criteria.page();
        int zeroBasedPage = page - 1; // Panache uses 0-based page indexing

        Uni<java.util.List<PipeDocRecord>> resultsUni = PipeDocRecord.<PipeDocRecord>find(query + orderBy, params.toArray())
                .page(zeroBasedPage, pageSize)
                .list();

        // Combine count and results
        return Uni.combine().all().unis(countUni, resultsUni)
                .asTuple()
                .map(tuple -> {
                    Long totalCount = tuple.getItem1();
                    java.util.List<PipeDocRecord> results = tuple.getItem2();
                    int totalPages = (int) Math.ceil((double) totalCount / pageSize);

                    return new DocumentSearchResult(
                            results,
                            totalCount,
                            page,
                            pageSize,
                            totalPages
                    );
                })
                .onFailure().transform(throwable -> {
                    LOG.errorf(throwable, "Failed to query documents by criteria");
                    return new DocumentQueryException("Failed to query documents by criteria", throwable);
                });
    }

    /**
     * Helper method to check if a filter value is valid (non-null and non-blank).
     */
    private boolean isValidFilterValue(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Search criteria for finding documents.
     */
    public record DocumentSearchCriteria(
            String datasourceId,
            String accountId,
            String connectorId,
            String clusterId,  // null = all, "" = intake only, "value" = specific cluster
            Instant createdAfter,
            Instant createdBefore,
            int page,
            int pageSize
    ) {
        public DocumentSearchCriteria {
            // Validation
            if (page < 1) {
                throw new IllegalArgumentException("Page must be >= 1, got: " + page);
            }
            if (pageSize < 1) {
                throw new IllegalArgumentException("Page size must be >= 1, got: " + pageSize);
            }
            if (pageSize > MAX_PAGE_SIZE) {
                throw new IllegalArgumentException("Page size must be <= " + MAX_PAGE_SIZE + ", got: " + pageSize);
            }
        }
    }

    /**
     * Result of a document search with pagination metadata.
     */
    public record DocumentSearchResult(
            java.util.List<PipeDocRecord> documents,
            long totalCount,
            int currentPage,
            int pageSize,
            int totalPages
    ) {}

    /**
     * Exception thrown when a document is not found.
     */
    public static class DocumentNotFoundException extends RuntimeException {
        public DocumentNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when a document query fails.
     */
    public static class DocumentQueryException extends RuntimeException {
        public DocumentQueryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when account validation fails.
     */
    public static class AccountValidationException extends RuntimeException {
        public AccountValidationException(String message) {
            super(message);
        }
    }
}