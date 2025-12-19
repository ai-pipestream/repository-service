package ai.pipestream.repository.http;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.ChecksumType;
import ai.pipestream.data.v1.FileStorageReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.s3.S3Config;
import io.quarkus.hibernate.orm.panache.Panache;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Phase 1 (Option 1): single-request HTTP upload.
 *
 * Intake proxies this request and hydrates identity/metadata via headers.
 * Repo-service is the only component that talks to S3 and persists the PipeDoc-for-parsing.
 */
@Path("/internal/uploads")
public class RawUploadResource {

    private static final Logger LOG = Logger.getLogger(RawUploadResource.class);

    @Inject
    S3AsyncClient s3;

    @Inject
    S3Config s3Config;

    /**
     * Raw octet-stream upload (single request).
     *
     * Receipt is returned after:
     * - S3 upload completes, AND
     * - DB commit completes for the PipeDocRecord.
     */
    @POST
    @Path("/raw")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public RawUploadReceipt uploadRaw(
            InputStream body,
            @HeaderParam("x-account-id") String accountId,
            @HeaderParam("x-connector-id") String connectorId,
            @HeaderParam("x-doc-id") String docId,
            @HeaderParam("x-drive-name") String driveName,
            @HeaderParam("x-filename") String filename,
            @HeaderParam("x-checksum-sha256") String checksumSha256,
            @HeaderParam("content-type") String contentType
    ) throws IOException {

        Objects.requireNonNull(body, "body must not be null");
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("x-account-id is required");
        }
        if (connectorId == null || connectorId.isBlank()) {
            throw new IllegalArgumentException("x-connector-id is required");
        }

        String resolvedDocId = (docId == null || docId.isBlank()) ? UUID.randomUUID().toString() : docId;
        String resolvedDriveName = (driveName == null || driveName.isBlank()) ? "default" : driveName;
        String resolvedFilename = (filename == null || filename.isBlank()) ? (resolvedDocId + ".bin") : filename;
        String resolvedContentType = (contentType == null || contentType.isBlank())
                ? MediaType.APPLICATION_OCTET_STREAM
                : contentType;

        // NOTE: For the very first cut we spool request bytes to disk to avoid holding the full payload in memory.
        // This keeps Option-1 semantics (single HTTP request) while staying low-memory.
        java.nio.file.Path tempFile = Files.createTempFile("repo-upload-", ".bin");
        long sizeBytes;
        try {
            sizeBytes = Files.copy(body, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            safeDelete(tempFile);
            throw e;
        }

        String objectKey = buildObjectKey(resolvedDriveName, accountId, connectorId, resolvedDocId, resolvedFilename);

        LOG.infof("Uploading doc_id=%s to s3://%s/%s (bytes=%d)", resolvedDocId, s3Config.bucket(), objectKey, sizeBytes);

        PutObjectResponse put = s3.putObject(
                        PutObjectRequest.builder()
                                .bucket(s3Config.bucket())
                                .key(objectKey)
                                .contentType(resolvedContentType)
                                .contentLength(sizeBytes)
                                .build(),
                        AsyncRequestBody.fromFile(tempFile)
                )
                .toCompletableFuture()
                .join();

        safeDelete(tempFile);

        String etag = put.eTag();
        String versionId = put.versionId();

        String resolvedChecksum = (checksumSha256 == null || checksumSha256.isBlank())
                ? ""
                : checksumSha256;

        PipeDoc pipeDoc = buildPipeDocForParsing(
                resolvedDocId,
                resolvedDriveName,
                objectKey,
                versionId,
                resolvedContentType,
                resolvedFilename,
                sizeBytes,
                resolvedChecksum
        );

        persistPipeDocRecord(
                resolvedDocId,
                resolvedDriveName,
                objectKey,
                versionId,
                etag,
                sizeBytes,
                resolvedContentType,
                resolvedFilename,
                resolvedChecksum,
                pipeDoc.toByteArray()
        );

        return new RawUploadReceipt(
                resolvedDocId,
                resolvedChecksum,
                resolvedDriveName,
                objectKey,
                versionId,
                etag,
                sizeBytes,
                resolvedContentType,
                resolvedFilename,
                "STORED_PIPEDOC"
        );
    }

    @Transactional
    void persistPipeDocRecord(String docId,
                              String driveName,
                              String objectKey,
                              String versionId,
                              String etag,
                              long sizeBytes,
                              String contentType,
                              String filename,
                              String checksum,
                              byte[] pipeDocBytes) {
        PipeDocRecord record = new PipeDocRecord();
        record.docId = docId;
        record.driveName = driveName;
        record.objectKey = objectKey;
        record.versionId = versionId;
        record.etag = etag;
        record.sizeBytes = sizeBytes;
        record.contentType = contentType;
        record.filename = filename;
        record.checksum = checksum == null ? "" : checksum;
        record.pipedocBytes = pipeDocBytes;
        record.createdAt = Instant.now();

        Panache.getEntityManager().persist(record);
    }

    private static PipeDoc buildPipeDocForParsing(String docId,
                                                  String driveName,
                                                  String objectKey,
                                                  String versionId,
                                                  String contentType,
                                                  String filename,
                                                  long sizeBytes,
                                                  String checksumSha256) {
        FileStorageReference.Builder ref = FileStorageReference.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey);
        if (versionId != null && !versionId.isBlank()) {
            ref.setVersionId(versionId);
        }

        Blob.Builder blob = Blob.newBuilder()
                .setBlobId(UUID.randomUUID().toString())
                .setDriveId(driveName)
                .setStorageRef(ref)
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

        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setBlobBag(BlobBag.newBuilder()
                        .setBlob(blob)
                        .build())
                .build();
    }

    private String buildObjectKey(String driveName, String accountId, String connectorId, String docId, String filename) {
        String safeDrive = sanitizePathSegment(driveName);
        String safeAccount = sanitizePathSegment(accountId);
        String safeConnector = sanitizePathSegment(connectorId);
        String safeDoc = sanitizePathSegment(docId);
        String safeName = sanitizeFilename(filename);
        return String.join("/",
                sanitizePathSegment(s3Config.keyPrefix()),
                safeDrive,
                safeAccount,
                safeConnector,
                safeDoc,
                safeName
        );
    }

    private static String sanitizePathSegment(String value) {
        if (value == null) return "unknown";
        String v = value.trim();
        if (v.isEmpty()) return "unknown";
        // Minimal safety: drop path separators
        return v.replace("/", "_").replace("\\", "_");
    }

    private static String sanitizeFilename(String value) {
        String v = sanitizePathSegment(value);
        // avoid empty or "." style names
        if (v.equals(".") || v.equals("..")) return "file.bin";
        return v;
    }

    private static void safeDelete(java.nio.file.Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (Exception e) {
            // best effort; do not fail request on temp cleanup
            LOG.debugf("Failed to delete temp file %s: %s", path, e.getMessage());
        }
    }
}


