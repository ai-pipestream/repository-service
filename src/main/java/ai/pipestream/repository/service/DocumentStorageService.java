package ai.pipestream.repository.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.account.AccountCacheService;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.kafka.RepositoryEventEmitter;
import ai.pipestream.repository.s3.S3Config;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.MutinyHelper;
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

    public DocumentStorageService() {
        LOG.info("DocumentStorageService initialized (Reactive)");
    }

    /**
     * Store a {@link PipeDoc}.
     *
     * Validates account, stores PipeDoc to S3, persists metadata to DB, emits event.
     *
     * @param document the document to store
     * @return Uni containing storage result containing the resolved document id and S3 key
     */
    public Uni<StoredDocument> store(PipeDoc document) {
        return store(document, null);
    }

    /**
     * Store a {@link PipeDoc} with a specific request ID for tracing.
     *
     * @param document the document to store
     * @param requestId optional request ID for tracing (auto-generated if null)
     * @return Uni containing storage result
     */
    public Uni<StoredDocument> store(PipeDoc document, String requestId) {
        // Capture the duplicated request context immediately
        Context requestContext = Vertx.currentContext().getDelegate();
        
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

                    String driveName = "default";
                    String objectKey = buildObjectKey(driveName, accountId, connectorId, finalDocId);

                    byte[] pipeDocBytes = docToStore.toByteArray();
                    String contentType = "application/x-protobuf";
                    long sizeBytes = pipeDocBytes.length;
                    String checksum = computeSha256(pipeDocBytes);

                    LOG.infof("Storing PipeDoc doc_id=%s to s3://%s/%s (bytes=%d)",
                            finalDocId, s3Config.bucket(), objectKey, sizeBytes);

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

                        // 3. Persist to DB (Reactive Transaction)
                        return Panache.withTransaction(() -> {
                            PipeDocRecord record = new PipeDocRecord();
                            record.docId = finalDocId;
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
                        }).map(persisted -> {
                            // 4. Emit Event
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

                            return new StoredDocument(finalDocId, objectKey, versionId, etag, sizeBytes, checksum);
                        });
                    });
                });
    }

    /**
     * Retrieve a PipeDoc by document ID.
     *
     * @param documentId the document ID
     * @return Uni containing the PipeDoc if found, or null/empty
     */
    public Uni<PipeDoc> get(String documentId) {
        Context requestContext = Vertx.currentContext().getDelegate();
        
        if (documentId == null || documentId.isBlank()) {
            return Uni.createFrom().nullItem();
        }

        // Look up metadata in DB
        return PipeDocRecord.<PipeDocRecord>find("docId", documentId).firstResult()
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
                            LOG.errorf(e, "Failed to parse PipeDoc from S3 for doc_id=%s", documentId);
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