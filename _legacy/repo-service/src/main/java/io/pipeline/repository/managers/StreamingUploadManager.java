package io.pipeline.repository.managers;

import com.google.protobuf.Any;
import io.smallrye.mutiny.Uni;
import io.pipeline.repository.services.S3Service;
import io.pipeline.streaming.upload.StreamingUploadService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Streaming upload manager that handles large file uploads without loading entire files into memory.
 * Uses S3 multipart upload with incremental SHA256 calculation.
 */
@ApplicationScoped
public class StreamingUploadManager implements StreamingUploadService {

    private static final Logger LOG = Logger.getLogger(StreamingUploadManager.class);

    @ConfigProperty(name = "repository.s3.multipart.threshold", defaultValue = "5242880")
    long multipartThreshold;

    @ConfigProperty(name = "repository.s3.multipart.chunk-size", defaultValue = "10485760") // 10MB
    int chunkSize;

    @Inject
    S3Service s3Service;

    @Inject
    PathManager pathManager;

    // Upload progress tracking
    private final ConcurrentHashMap<String, StreamingUploadSession> activeUploads = new ConcurrentHashMap<>();

    /**
     * Upload a protobuf Any object using streaming (no full file in memory).
     * This method is designed for large files that should not be loaded entirely into memory.
     */
    public Uni<UploadResult> uploadProtobufStreaming(String bucketName, String connectorId, String path,
                                                   String fileName, Any protobufPayload,
                                                   Map<String, String> metadata) {

        byte[] data = protobufPayload.toByteArray();
        String contentType = "application/x-protobuf";

        // Add protobuf type to metadata
        Map<String, String> enhancedMetadata = new java.util.HashMap<>(metadata);
        enhancedMetadata.put("proto-type", protobufPayload.getTypeUrl());
        enhancedMetadata.put("filename", fileName);

        return uploadBytesStreaming(bucketName, connectorId, path, fileName, data, contentType, enhancedMetadata);
    }

    /**
     * Upload bytes using streaming approach (no full file in memory for large files).
     * For files smaller than multipartThreshold, uses direct upload.
     * For larger files, uses streaming multipart upload with incremental hashing.
     */
    public Uni<UploadResult> uploadBytesStreaming(String bucketName, String connectorId, String path,
                                                String fileName, byte[] data, String contentType,
                                                Map<String, String> metadata) {

        String nodeId = UUID.randomUUID().toString();
        String s3Key = pathManager.buildS3Key(connectorId, path, nodeId);

        LOG.infof("Streaming upload: nodeId=%s, size=%d bytes, s3Key=%s", nodeId, data.length, s3Key);

        if (data.length < multipartThreshold) {
            // Small file - direct upload (still need to calculate hash)
            return Uni.createFrom().item(() -> {
                String contentHash = calculateSHA256Hash(data);
                S3Service.S3ObjectMetadata s3Metadata = s3Service.putObject(bucketName, s3Key, data, contentType, metadata);
                return new UploadResult(nodeId, s3Key, data.length, s3Metadata.eTag, s3Metadata.versionId, false, contentHash);
            });
        } else {
            // Large file - streaming multipart upload
            return uploadLargeFileStreaming(bucketName, s3Key, nodeId, data, contentType, metadata);
        }
    }

    /**
     * Upload large file using streaming multipart upload with incremental hashing.
     */
    private Uni<UploadResult> uploadLargeFileStreaming(String bucketName, String s3Key, String nodeId,
                                                     byte[] data, String contentType, Map<String, String> metadata) {
        
        return Uni.createFrom().item(() -> {
            try (InputStream inputStream = new ByteArrayInputStream(data)) {
                // 1. Initiate multipart upload
                String uploadId = s3Service.initiateMultipartUpload(bucketName, nodeId, 
                    pathManager.parseS3Key(s3Key).connectorId, contentType, metadata);

                // 2. Stream upload with incremental hashing
                StreamingUploadSession session = new StreamingUploadSession(uploadId, s3Key, data.length);
                activeUploads.put(nodeId, session);

                List<String> etags = new ArrayList<>();
                int partNumber = 1;
                byte[] buffer = new byte[chunkSize];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    // Create part data (only the bytes we read)
                    byte[] partData = new byte[bytesRead];
                    System.arraycopy(buffer, 0, partData, 0, bytesRead);

                    // Update hash incrementally
                    session.updateHash(partData);

                    // Upload part
                    String etag = s3Service.uploadPart(bucketName, nodeId, 
                        pathManager.parseS3Key(s3Key).connectorId, uploadId, partNumber, partData);
                    etags.add(etag);

                    LOG.debugf("Uploaded part %d: %d bytes, etag=%s", partNumber, bytesRead, etag);
                    partNumber++;
                }

                // 3. Complete multipart upload
                S3Service.S3ObjectMetadata s3Metadata = s3Service.completeMultipartUpload(
                    bucketName, nodeId, pathManager.parseS3Key(s3Key).connectorId, uploadId, etags);

                // 4. Clean up session
                activeUploads.remove(nodeId);

                return new UploadResult(nodeId, s3Key, data.length, s3Metadata.eTag, 
                    s3Metadata.versionId, true, session.getFinalHash());

            } catch (IOException e) {
                // Clean up on error
                activeUploads.remove(nodeId);
                throw new RuntimeException("Failed to stream upload: " + e.getMessage(), e);
            }
        }).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Calculate SHA256 hash of byte array (for small files).
     */
    private String calculateSHA256Hash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            LOG.error("SHA-256 algorithm not available", e);
            return "unknown";
        }
    }

    /**
     * Convert bytes to hex string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    /**
     * Session for tracking streaming upload progress and incremental hashing.
     */
    private static class StreamingUploadSession {
        private final String uploadId;
        private final String s3Key;
        private final long totalSize;
        private final MessageDigest digest;
        private long bytesProcessed = 0;

        public StreamingUploadSession(String uploadId, String s3Key, long totalSize) {
            this.uploadId = uploadId;
            this.s3Key = s3Key;
            this.totalSize = totalSize;
            try {
                this.digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-256 not available", e);
            }
        }

        public void updateHash(byte[] data) {
            digest.update(data);
            bytesProcessed += data.length;
        }

        public String getFinalHash() {
            byte[] hash = digest.digest();
            StringBuilder result = new StringBuilder();
            for (byte b : hash) {
                result.append(String.format("%02x", b));
            }
            return result.toString();
        }

        public double getProgress() {
            return (double) bytesProcessed / totalSize;
        }
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String nodeId, String connectorId, 
                                        String contentType, Map<String, String> metadata) {
        return s3Service.initiateMultipartUpload(bucketName, nodeId, connectorId, contentType, metadata);
    }

    @Override
    public String uploadPart(String bucketName, String nodeId, String connectorId, 
                           String uploadId, int partNumber, byte[] data) {
        return s3Service.uploadPart(bucketName, nodeId, connectorId, uploadId, partNumber, data);
    }

    @Override
    public UploadMetadata completeMultipartUpload(String bucketName, String nodeId, 
                                                String connectorId, String uploadId, 
                                                List<String> etags) {
        S3Service.S3ObjectMetadata s3Metadata = s3Service.completeMultipartUpload(bucketName, nodeId, connectorId, uploadId, etags);
        return new UploadMetadata(nodeId, s3Metadata.s3Key, s3Metadata.size, s3Metadata.eTag, 
                                s3Metadata.versionId, true, null); // Hash calculated separately
    }

    /**
     * Upload result containing metadata about the uploaded file.
     */
    public static class UploadResult {
        public final String nodeId;
        public final String s3Key;
        public final long size;
        public final String eTag;
        public final String versionId;
        public final boolean wasMultipart;
        public final String contentHash;

        public UploadResult(String nodeId, String s3Key, long size, String eTag, 
                          String versionId, boolean wasMultipart, String contentHash) {
            this.nodeId = nodeId;
            this.s3Key = s3Key;
            this.size = size;
            this.eTag = eTag;
            this.versionId = versionId;
            this.wasMultipart = wasMultipart;
            this.contentHash = contentHash;
        }
    }
}