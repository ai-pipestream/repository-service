package io.pipeline.repository.managers;

import com.google.protobuf.Any;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.pipeline.repository.services.S3Service;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core upload manager that handles streaming uploads with automatic chunking.
 * Supports both small direct uploads and large chunked uploads transparently.
 */
@ApplicationScoped
public class UploadManager {

    private static final Logger LOG = Logger.getLogger(UploadManager.class);

    public enum ChunkSizeConfig {
        SMALL(5 * 1024 * 1024),      // 5MB - S3 minimum
        MEDIUM(10 * 1024 * 1024),    // 10MB - good default
        LARGE(50 * 1024 * 1024),     // 50MB - high bandwidth
        XLARGE(100 * 1024 * 1024);   // 100MB - max reasonable

        private final int bytes;

        ChunkSizeConfig(int bytes) {
            this.bytes = bytes;
        }

        public int getBytes() {
            return bytes;
        }

        public int getMaxBufferSize() {
            return bytes * 2;  // 2x for safety
        }
    }

    @ConfigProperty(name = "repository.s3.multipart.threshold", defaultValue = "5242880")
    long multipartThreshold;

    @ConfigProperty(name = "repository.s3.multipart.chunk-size", defaultValue = "MEDIUM")
    ChunkSizeConfig chunkSizeConfig;

    @Inject
    S3Service s3Service;

    @Inject
    PathManager pathManager;

    @Inject
    StreamingUploadManager streamingUploadManager;

    // Upload progress tracking
    private final ConcurrentHashMap<String, UploadProgress> activeUploads = new ConcurrentHashMap<>();

    /**
     * Upload protobuf Any object (primary method)
     * Uses streaming for large files to avoid memory issues.
     */
    public Uni<UploadResult> uploadProtobuf(String bucketName, String connectorId, String path,
                                           String fileName, Any protobufPayload,
                                           Map<String, String> metadata) {

        byte[] data = protobufPayload.toByteArray();
        String contentType = "application/x-protobuf";

        // Add protobuf type to metadata
        Map<String, String> enhancedMetadata = new java.util.HashMap<>(metadata);
        enhancedMetadata.put("proto-type", protobufPayload.getTypeUrl());
        enhancedMetadata.put("filename", fileName);

        // Use streaming for large files to avoid memory issues
        if (data.length > multipartThreshold) {
            LOG.infof("Using streaming upload for large file: %s (%d bytes)", fileName, data.length);
            return streamingUploadManager.uploadProtobufStreaming(bucketName, connectorId, path, fileName, protobufPayload, enhancedMetadata)
                .map(result -> new UploadResult(result.nodeId, result.s3Key, result.size, result.eTag, result.versionId, result.wasMultipart, result.contentHash));
        } else {
            return uploadBytes(bucketName, connectorId, path, fileName, data, contentType, enhancedMetadata);
        }
    }

    /**
     * Upload bytes directly (for raw files)
     */
    public Uni<UploadResult> uploadBytes(String bucketName, String connectorId, String path,
                                        String fileName, byte[] data, String contentType,
                                        Map<String, String> metadata) {

        String nodeId = UUID.randomUUID().toString();
        String s3Key = pathManager.buildS3Key(connectorId, path, nodeId);

        LOG.infof("Direct upload: nodeId=%s, size=%d bytes, s3Key=%s", nodeId, data.length, s3Key);

        if (data.length < multipartThreshold) {
            // Small file - direct upload
            return Uni.createFrom().item(() -> {
                // Calculate content hash
                String contentHash = calculateSHA256Hash(data);

                S3Service.S3ObjectMetadata s3Metadata = s3Service.putObject(bucketName, s3Key, data, contentType, metadata);
                return new UploadResult(nodeId, s3Key, data.length, s3Metadata.eTag, s3Metadata.versionId, false, contentHash);
            });
        } else {
            // Large file - use multipart upload
            return Uni.createFrom().item(() -> {
                // Calculate content hash for large file
                String contentHash = calculateSHA256Hash(data);

                // 1. Initiate multipart upload
                String s3UploadId = s3Service.initiateMultipartUpload(bucketName, nodeId,
                    pathManager.parseS3Key(s3Key).connectorId, contentType, metadata);

                // 2. Upload parts
                List<String> etags = new ArrayList<>();
                int chunkSize = chunkSizeConfig.getBytes();
                int partNumber = 1;

                for (int offset = 0; offset < data.length; offset += chunkSize) {
                    int endIndex = Math.min(offset + chunkSize, data.length);
                    byte[] partData = java.util.Arrays.copyOfRange(data, offset, endIndex);

                    String etag = s3Service.uploadPart(bucketName, nodeId,
                        pathManager.parseS3Key(s3Key).connectorId, s3UploadId, partNumber, partData);
                    etags.add(etag);
                    partNumber++;
                }

                // 3. Complete multipart upload
                S3Service.S3ObjectMetadata result = s3Service.completeMultipartUpload(
                    bucketName, nodeId, pathManager.parseS3Key(s3Key).connectorId, s3UploadId, etags);

                return new UploadResult(nodeId, result.s3Key, data.length, result.eTag, result.versionId, true, contentHash);
            });
        }
    }

    /**
     * Initiate streaming upload (for chunked uploads from gRPC)
     */
    public Uni<InitiateUploadResult> initiateUpload(String bucketName, String connectorId,
                                                   String path, String fileName, long expectedSize,
                                                   String contentType, Map<String, String> metadata) {

        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();
        String s3Key = pathManager.buildS3Key(connectorId, path, nodeId);

        LOG.infof("Initiating upload: nodeId=%s, expectedSize=%d, s3Key=%s", nodeId, expectedSize, s3Key);

        // Create upload progress
        UploadProgress progress = new UploadProgress(nodeId, uploadId, bucketName, s3Key,
                                                    connectorId, expectedSize, chunkSizeConfig);
        activeUploads.put(nodeId, progress);

        if (expectedSize < multipartThreshold) {
            // Will be direct upload when chunks arrive
            progress.useMultipart = false;
            return Uni.createFrom().item(new InitiateUploadResult(nodeId, uploadId, false));
        } else {
            // Initiate S3 multipart upload
            return Uni.createFrom().item(() -> {
                String s3UploadId = s3Service.initiateMultipartUpload(bucketName, nodeId, connectorId, contentType, metadata);
                progress.s3UploadId = s3UploadId;
                progress.useMultipart = true;
                return new InitiateUploadResult(nodeId, uploadId, true);
            });
        }
    }

    /**
     * Process streaming chunks (from gRPC streaming)
     */
    public Uni<UploadResult> processChunks(Multi<ChunkData> chunks) {
        return chunks
            .collect().first()
            .flatMap(firstChunk -> {
                UploadProgress progress = activeUploads.get(firstChunk.nodeId);
                if (progress == null) {
                    return Uni.createFrom().failure(new RuntimeException("Upload not found: " + firstChunk.nodeId));
                }

                if (progress.useMultipart) {
                    return processMultipartChunks(chunks, progress);
                } else {
                    return processDirectChunks(chunks, progress);
                }
            });
    }

    private Uni<UploadResult> uploadMultipart(String bucketName, String connectorId, String nodeId,
                                             byte[] data, String contentType, Map<String, String> metadata) {
        return Uni.createFrom().item(() -> {
            String s3UploadId = s3Service.initiateMultipartUpload(bucketName, nodeId, connectorId, contentType, metadata);

            // Split into chunks and upload
            List<String> etags = uploadPartsSync(bucketName, nodeId, connectorId, s3UploadId, data);

            // Complete upload
            S3Service.S3ObjectMetadata result = s3Service.completeMultipartUpload(bucketName, nodeId, connectorId, s3UploadId, etags);

            return new UploadResult(nodeId, result.s3Key, data.length, result.eTag, result.versionId, true, "");
        });
    }

    private List<String> uploadPartsSync(String bucketName, String nodeId, String connectorId,
                                        String uploadId, byte[] data) {
        List<String> etags = new ArrayList<>();
        int chunkSize = chunkSizeConfig.getBytes();
        int partNumber = 1;

        for (int offset = 0; offset < data.length; offset += chunkSize) {
            int endIndex = Math.min(offset + chunkSize, data.length);
            byte[] partData = java.util.Arrays.copyOfRange(data, offset, endIndex);

            String etag = s3Service.uploadPart(bucketName, nodeId, connectorId, uploadId, partNumber, partData);
            etags.add(etag);

            LOG.debugf("Uploaded part %d: nodeId=%s, size=%d, etag=%s",
                partNumber, nodeId, partData.length, etag);

            partNumber++;
        }

        LOG.infof("Completed multipart upload parts: nodeId=%s, totalParts=%d", nodeId, etags.size());
        return etags;
    }

    private Uni<UploadResult> processMultipartChunks(Multi<ChunkData> chunks, UploadProgress progress) {
        // Implementation for multipart chunk processing
        // (Extract from existing NodeUploadServiceImpl)
        return Uni.createFrom().nullItem(); // TODO: implement
    }

    private Uni<UploadResult> processDirectChunks(Multi<ChunkData> chunks, UploadProgress progress) {
        // Collect all chunks and do direct upload
        return chunks.collect().asList()
            .map(chunkList -> {
                // Combine chunks
                byte[] combinedData = combineChunks(chunkList);

                // Direct upload
                S3Service.S3ObjectMetadata s3Metadata = s3Service.putObject(
                    progress.bucketName, progress.s3Key, combinedData, "application/octet-stream", Map.of());

                // Cleanup
                activeUploads.remove(progress.nodeId);

                return new UploadResult(progress.nodeId, progress.s3Key, combinedData.length,
                                      s3Metadata.eTag, s3Metadata.versionId, false, "");
            });
    }

    private byte[] combineChunks(List<ChunkData> chunks) {
        // Implementation to combine chunk data
        return new byte[0]; // TODO: implement
    }

    private String calculateSHA256Hash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(data);
            return java.util.Base64.getEncoder().encodeToString(hashBytes);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to calculate SHA-256 hash");
            return "";
        }
    }

    // Result classes
    public static class UploadResult {
        public final String nodeId;
        public final String s3Key;
        public final long size;
        public final String eTag;
        public final String versionId;
        public final boolean wasMultipart;
        public final String contentHash;

        public UploadResult(String nodeId, String s3Key, long size, String eTag, String versionId, boolean wasMultipart, String contentHash) {
            this.nodeId = nodeId;
            this.s3Key = s3Key;
            this.size = size;
            this.eTag = eTag;
            this.versionId = versionId;
            this.wasMultipart = wasMultipart;
            this.contentHash = contentHash;
        }
    }

    public static class InitiateUploadResult {
        public final String nodeId;
        public final String uploadId;
        public final boolean willUseMultipart;

        public InitiateUploadResult(String nodeId, String uploadId, boolean willUseMultipart) {
            this.nodeId = nodeId;
            this.uploadId = uploadId;
            this.willUseMultipart = willUseMultipart;
        }
    }

    public static class ChunkData {
        public final String nodeId;
        public final String uploadId;
        public final int chunkNumber;
        public final byte[] data;
        public final boolean isLast;

        public ChunkData(String nodeId, String uploadId, int chunkNumber, byte[] data, boolean isLast) {
            this.nodeId = nodeId;
            this.uploadId = uploadId;
            this.chunkNumber = chunkNumber;
            this.data = data;
            this.isLast = isLast;
        }
    }

    // Internal progress tracking
    private static class UploadProgress {
        final String nodeId;
        final String uploadId;
        final String bucketName;
        final String s3Key;
        final String connectorId;
        final long expectedSize;
        final ChunkSizeConfig chunkConfig;
        String s3UploadId;
        boolean useMultipart;
        MessageDigest digest;

        UploadProgress(String nodeId, String uploadId, String bucketName, String s3Key,
                      String connectorId, long expectedSize, ChunkSizeConfig chunkConfig) {
            this.nodeId = nodeId;
            this.uploadId = uploadId;
            this.bucketName = bucketName;
            this.s3Key = s3Key;
            this.connectorId = connectorId;
            this.expectedSize = expectedSize;
            this.chunkConfig = chunkConfig;
        }
    }
}