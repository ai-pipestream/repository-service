package ai.pipestream.repository.http;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.ChecksumType;
import ai.pipestream.data.v1.FileStorageReference;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.account.AccountCacheService;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.kafka.RepositoryEventEmitter;
import ai.pipestream.repository.s3.S3Config;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Phase 1 (Option 1): single-request HTTP upload.
 *
 * Intake proxies this request and hydrates identity/metadata via headers.
 * Repo-service is the only component that talks to S3 and persists the PipeDoc-for-parsing.
 *
 * Reactive Implementation - Zero Disk IO.
 */
@Path("/internal/uploads")
public class RawUploadResource {

    private static final Logger LOG = Logger.getLogger(RawUploadResource.class);

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

    /**
     * Raw octet-stream upload (single request).
     *
     * Receipt is returned after:
     * - S3 upload completes (Streamed directly, no disk buffering), AND
     * - DB commit completes for the PipeDocRecord.
     */
    @POST
    @Path("/raw")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<RawUploadReceipt> uploadRaw(
            InputStream body,
            @HeaderParam("Content-Length") Long contentLength,
            @HeaderParam("x-account-id") String accountId,
            @HeaderParam("x-connector-id") String connectorId,
            @HeaderParam("x-doc-id") String docId,
            @HeaderParam("x-drive-name") String driveName,
            @HeaderParam("x-filename") String filename,
            @HeaderParam("x-checksum-sha256") String checksumSha256,
            @HeaderParam("x-request-id") String requestId,
            @HeaderParam("content-type") String contentType
    ) {
        // Capture context for restoring after async S3 operations
        io.vertx.core.Context requestContext = io.vertx.mutiny.core.Vertx.currentContext().getDelegate();

        if (body == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("body must not be null"));
        }
        if (contentLength == null || contentLength <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("Content-Length header is required"));
        }
        if (accountId == null || accountId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("x-account-id is required"));
        }
        if (connectorId == null || connectorId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("x-connector-id is required"));
        }

        // 1. Validate Account (Reactive)
        return accountCacheService.isValidAccount(accountId)
                .flatMap(isValid -> {
                    if (!isValid) {
                        LOG.warnf("Rejected upload: invalid or inactive account_id=%s", accountId);
                        return Uni.createFrom().failure(new WebApplicationException(
                                Response.status(Response.Status.FORBIDDEN)
                                        .entity("{\"error\": \"Account not found or inactive: " + accountId + "\"}")
                                        .type(MediaType.APPLICATION_JSON)
                                        .build()));
                    }

                    // Prepare metadata
                    String resolvedRequestId = (requestId == null || requestId.isBlank())
                            ? UUID.randomUUID().toString()
                            : requestId;
                    String resolvedConnectorId = (connectorId == null || connectorId.isBlank()) ? null : connectorId;
                    String datasourceId = computeDatasourceId(accountId, resolvedConnectorId);
                    String resolvedDocId = (docId == null || docId.isBlank()) ? UUID.randomUUID().toString() : docId;
                    String resolvedDriveName = (driveName == null || driveName.isBlank()) ? "default" : driveName;
                    String resolvedFilename = (filename == null || filename.isBlank()) ? (resolvedDocId + ".bin") : filename;
                    String resolvedContentType = (contentType == null || contentType.isBlank())
                            ? MediaType.APPLICATION_OCTET_STREAM
                            : contentType;

                    // Generate deterministic UUID for node_id using (doc_id, datasource_id, account_id)
                    // This is needed for the new path structure: intake/{uuid}.pb
                    UUID nodeId = uuidGenerator.generateNodeId(resolvedDocId, datasourceId, accountId);
                    
                    // Generate UUID for blob filename (using blob_id from Blob proto)
                    // Blobs are stored with UUID-based filenames to avoid filename collisions and OS-specific issues
                    UUID blobId = UUID.randomUUID();
                    
                    // Build S3 object keys using new path structure:
                    // Raw blob: {prefix}/{drive}/{account}/{connector}/{datasource}/{docId}/intake/{blob-uuid}.bin
                    // PipeDoc: {prefix}/{drive}/{account}/{connector}/{datasource}/{docId}/intake/{node-uuid}.pb
                    String basePath = buildObjectKeyBaseForIntake(resolvedDriveName, accountId, resolvedConnectorId, datasourceId, resolvedDocId);
                    String blobObjectKey = basePath + "/" + blobId.toString() + ".bin";  // Use UUID for blob filename
                    String pipedocObjectKey = basePath + "/" + nodeId.toString() + ".pb";
                    
                    LOG.infof("Uploading doc_id=%s, blob_id=%s to s3://%s/%s (bytes=%d), PipeDoc will be at %s", 
                            resolvedDocId, blobId, s3Config.bucket(), blobObjectKey, contentLength, pipedocObjectKey);

                    // 2. Upload Raw File to S3 (Streamed Async)
                    // We use the Worker Pool executor to read from the blocking InputStream
                    ExecutorService executor = (ExecutorService) Infrastructure.getDefaultWorkerPool();
                    
                    return Uni.createFrom().completionStage(
                            s3AsyncClient.putObject(
                                    PutObjectRequest.builder()
                                            .bucket(s3Config.bucket())
                                            .key(blobObjectKey)
                                            .contentType(resolvedContentType)
                                            .contentLength(contentLength)
                                            .build(),
                                    AsyncRequestBody.fromInputStream(body, contentLength, executor)
                            )
                    ).flatMap(putResponse -> {
                        String etag = putResponse.eTag();
                        String versionId = putResponse.versionId();
                        String resolvedChecksum = (checksumSha256 == null || checksumSha256.isBlank()) ? "" : checksumSha256;

                        PipeDoc pipeDoc = buildPipeDocForParsing(
                                resolvedDocId,
                                accountId,
                                datasourceId,
                                resolvedConnectorId,
                                resolvedDriveName,
                                blobObjectKey,
                                blobId.toString(),  // Pass blob_id so it's used consistently
                                versionId,
                                resolvedContentType,
                                resolvedFilename,  // Original filename stored as metadata
                                contentLength,
                                resolvedChecksum
                        );
                        
                        // PipeDoc object key is already set above using new path structure
                        byte[] pipeDocBytes = pipeDoc.toByteArray();

                        // 3. Store PipeDoc protobuf to S3 (Async, Memory-based)
                        return Uni.createFrom().completionStage(
                                s3AsyncClient.putObject(
                                        PutObjectRequest.builder()
                                                .bucket(s3Config.bucket())
                                                .key(pipedocObjectKey)
                                                .contentType("application/x-protobuf")
                                                .contentLength((long) pipeDocBytes.length)
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
                        .flatMap(pipeDocResponse -> {
                            
                        // 4. Persist Metadata (Reactive Transaction)
                        // For initial intake, graph_address_id = datasource_id, cluster_id = null
                        return Panache.withTransaction(() -> 
                            PipeDocRecord.<PipeDocRecord>findById(nodeId)
                                .flatMap(existingRecord -> {
                                    if (existingRecord != null) {
                                        LOG.debugf("Updating existing PipeDocRecord for idempotent replay: node_id=%s, doc_id=%s", nodeId, resolvedDocId);
                                        existingRecord.objectKey = blobObjectKey;
                                        existingRecord.pipedocObjectKey = pipedocObjectKey;
                                        existingRecord.clusterId = null;
                                        existingRecord.versionId = versionId;
                                        existingRecord.etag = etag;
                                        existingRecord.sizeBytes = contentLength;
                                        existingRecord.contentType = resolvedContentType;
                                        existingRecord.filename = resolvedFilename;
                                        existingRecord.checksum = resolvedChecksum;
                                        return existingRecord.persist();
                                    }

                                    LOG.debugf("Creating new PipeDocRecord: node_id=%s, doc_id=%s", nodeId, resolvedDocId);
                                    PipeDocRecord record = new PipeDocRecord();
                                    record.nodeId = nodeId; // UUID already generated above
                                    record.docId = resolvedDocId;
                                    record.graphAddressId = datasourceId; // For initial intake, graph_address_id = datasource_id
                                    record.clusterId = null; // Intake documents don't belong to a cluster
                                    record.accountId = accountId;
                                    record.datasourceId = datasourceId;
                                    record.connectorId = resolvedConnectorId;
                                    record.driveName = resolvedDriveName;
                                    record.objectKey = blobObjectKey; // Raw blob path with UUID filename
                                    record.pipedocObjectKey = pipedocObjectKey; // PipeDoc path with new structure
                                    record.versionId = versionId;
                                    record.etag = etag;
                                    record.sizeBytes = contentLength;
                                    record.contentType = resolvedContentType;
                                    record.filename = resolvedFilename;
                                    record.checksum = resolvedChecksum;
                                    record.createdAt = Instant.now();
                                    return record.persist();
                                })
                        );
                        }).map(persisted -> {
                            // 5. Emit Event
                            eventEmitter.emitCreated(
                                resolvedDocId,
                                accountId,
                                blobObjectKey,
                                pipedocObjectKey,
                                    contentLength,
                                    resolvedChecksum,
                                    s3Config.bucket(),
                                    versionId,
                                    resolvedRequestId,
                                resolvedConnectorId,
                                datasourceId
                            );

                            return new RawUploadReceipt(
                                    resolvedDocId,
                                    resolvedChecksum,
                                    resolvedDriveName,
                                    blobObjectKey,  // Use blobObjectKey (UUID-based path)
                                    versionId,
                                    etag,
                                    contentLength,
                                    resolvedContentType,
                                    resolvedFilename,  // Original filename preserved in receipt and blob metadata
                                    "STORED_PIPEDOC"
                            );
                        });
                    });
                });
    }

    private static PipeDoc buildPipeDocForParsing(String docId,
                                                  String accountId,
                                                  String datasourceId,
                                                  String connectorId,
                                                  String driveName,
                                                  String objectKey,  // S3 object key (UUID-based path)
                                                  String blobId,  // UUID for blob_id field in Blob proto
                                                  String versionId,
                                                  String contentType,
                                                  String filename,  // Original filename (stored as metadata in blob.filename)
                                                  long sizeBytes,
                                                  String checksumSha256) {
        FileStorageReference.Builder ref = FileStorageReference.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey);
        if (versionId != null && !versionId.isBlank()) {
            ref.setVersionId(versionId);
        }

        Blob.Builder blob = Blob.newBuilder()
                .setBlobId(blobId)  // Use the provided blob_id (UUID) - matches the UUID used in S3 filename
                .setDriveId(driveName)
                .setStorageRef(ref)  // Points to S3 object key (UUID-based path)
                .setSizeBytes(sizeBytes);

        if (contentType != null && !contentType.isBlank()) {
            blob.setMimeType(contentType);
        }
        if (filename != null && !filename.isBlank()) {
            blob.setFilename(filename);
        }
        if (checksumSha256 != null && !checksumSha256.isBlank()) {
            blob.setChecksum(checksumSha256);
            blob.setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256);
        }

        OwnershipContext.Builder ownership = OwnershipContext.newBuilder()
                .setAccountId(accountId)
                .setDatasourceId(datasourceId);
        if (connectorId != null && !connectorId.isBlank()) {
            ownership.setConnectorId(connectorId);
        }

        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(ownership)
                .setBlobBag(BlobBag.newBuilder()
                        .setBlob(blob)
                        .build())
                .build();
    }

    /**
     * Compute deterministic datasource ID from account + connector.
     * Format: first 16 chars of SHA-256(accountId + ":" + connectorId)
     */
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

    /**
     * Builds the base path for intake documents: {prefix}/{drive}/{account}/{connector}/{datasource}/{docId}/intake
     * Used for constructing both raw blob and PipeDoc paths.
     */
    private String buildObjectKeyBaseForIntake(String driveName, String accountId, String connectorId, String datasourceId, String docId) {
        String safeDrive = sanitizePathSegment(driveName);
        String safeAccount = sanitizePathSegment(accountId);
        String safeConnector = sanitizePathSegment(connectorId);
        String safeDatasource = sanitizePathSegment(datasourceId);
        String safeDoc = sanitizePathSegment(docId);
        
        return String.join("/",
                sanitizePathSegment(s3Config.keyPrefix()),
                safeDrive,
                safeAccount,
                safeConnector,
                safeDatasource,
                safeDoc,
                "intake"
        );
    }
    

    private static String sanitizePathSegment(String value) {
        if (value == null) return "unknown";
        String v = value.trim();
        if (v.isEmpty()) return "unknown";
        return v.replace("/", "_").replace("\\", "_");
    }

    private static String sanitizeFilename(String value) {
        String v = sanitizePathSegment(value);
        if (v.equals(".") || v.equals("..")) return "file.bin";
        return v;
    }
}