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
    ai.pipestream.repository.graph.GraphValidationService graphValidationService;

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
        return store(document, null, null);
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
        return store(document, requestId, null);
    }

    /**
     * Store a {@link PipeDoc} with a specific request ID and graph location ID.
     *
     * @param document the document to store
     * @param requestId optional request ID for tracing (auto-generated if null)
     * @param graphLocationId optional graph location ID (if null, uses datasource_id from ownership)
     * @return Uni containing storage result
     */
    public Uni<StoredDocument> store(PipeDoc document, String requestId, String graphLocationId) {
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
                    if (graphLocationId != null && !graphLocationId.isBlank()) {
                        // Validate graph location ID against graph service (topology correctness check)
                        return graphValidationService.validateGraphLocation(graphLocationId)
                                .flatMap(isGraphLocationValid -> {
                                    if (!isGraphLocationValid) {
                                        return Uni.createFrom().failure(new IllegalArgumentException(
                                                "Invalid graph_location_id: " + graphLocationId));
                                    }
                                    return continueStoring(docToStore, finalDocId, accountId, connectorId, finalDatasourceId,
                                            resolvedRequestId, graphLocationId);
                                });
                    } else {
                        // Use datasource_id as graph_address_id (for initial intake)
                        return continueStoring(docToStore, finalDocId, accountId, connectorId, finalDatasourceId,
                                resolvedRequestId, finalDatasourceId);
                    }
                });
    }

    /**
     * Continues the storage process after graph address ID is resolved.
     */
    private Uni<StoredDocument> continueStoring(PipeDoc docToStore, String finalDocId, String accountId, 
            String connectorId, String finalDatasourceId, String resolvedRequestId, String resolvedGraphAddressId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        
        String driveName = "default";
        String objectKey = buildObjectKey(driveName, accountId, connectorId, finalDocId);

        byte[] pipeDocBytes = docToStore.toByteArray();
        String contentType = "application/x-protobuf";
        long sizeBytes = pipeDocBytes.length;
        String checksum = computeSha256(pipeDocBytes);

        LOG.infof("Storing PipeDoc doc_id=%s, graph_address_id=%s to s3://%s/%s (bytes=%d)",
                finalDocId, resolvedGraphAddressId, s3Config.bucket(), objectKey, sizeBytes);

        // 2. Store to S3
        return Uni.createFrom().completionStage(
                s3AsyncClient.putObject(
                        PutObjectRequest.builder()
                                .bucket(s3Config.bucket())
                                .key(objectKey)
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

            // 3. Generate deterministic UUID for node_id using (doc_id, graph_address_id, account_id)
            UUID nodeId = uuidGenerator.generateNodeId(finalDocId, resolvedGraphAddressId, accountId);
            
            // 4. Persist to DB (Reactive Transaction with UPSERT pattern)
            return Panache.withTransaction(() -> 
                PipeDocRecord.<PipeDocRecord>findById(nodeId)
                        .flatMap(existingRecord -> {
                            if (existingRecord != null) {
                                // UPDATE existing record (new version)
                                LOG.debugf("Updating existing PipeDocRecord: node_id=%s", nodeId);
                                existingRecord.objectKey = objectKey;
                                existingRecord.pipedocObjectKey = objectKey;
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
                                record.accountId = accountId;
                                record.datasourceId = finalDatasourceId;
                                record.connectorId = connectorId;
                                record.driveName = driveName;
                                record.objectKey = objectKey;
                                record.pipedocObjectKey = objectKey;
                                record.versionId = versionId;
                                record.etag = etag;
                                record.sizeBytes = sizeBytes;
                                record.contentType = contentType;
                                record.filename = finalDocId + ".pb";
                                record.checksum = checksum == null ? "" : checksum;
                                record.createdAt = Instant.now();
                                return record.persist();
                            }
                        })
            ).map(persisted -> {
                // 5. Emit Event
                eventEmitter.emitCreated(
                        finalDocId,
                        accountId,
                        objectKey,
                        objectKey,
                        sizeBytes,
                        checksum,
                        s3Config.bucket(),
                        versionId,
                        resolvedRequestId,
                        connectorId
                );

                // Return node_id (UUID) as the repository identifier
                return new StoredDocument(nodeId.toString(), objectKey, versionId, etag, sizeBytes, checksum);
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

    private String buildObjectKey(String driveName, String accountId, String connectorId, String docId) {
        String safeDrive = sanitize(driveName);
        String safeAccount = sanitize(accountId);
        String safeConnector = sanitize(connectorId);
        String safeDoc = sanitize(docId);
        return String.join("/",
                sanitize(s3Config.keyPrefix()),
                safeDrive,
                safeAccount,
                safeConnector,
                safeDoc,
                safeDoc + ".pb"
        );
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
            String checksum
    ) {}

    /**
     * Exception thrown when account validation fails.
     */
    public static class AccountValidationException extends RuntimeException {
        public AccountValidationException(String message) {
            super(message);
        }
    }
}