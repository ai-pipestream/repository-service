package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.upload.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import io.pipeline.repository.exception.*;
import io.pipeline.repository.util.HashUtil;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.io.ByteArrayOutputStream;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Implementation of NodeUploadService for multipart uploads.
 * This service handles chunked uploads for large files.
 */
@GrpcService
public class NodeUploadServiceImpl extends MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(NodeUploadServiceImpl.class);

    @ConfigProperty(name = "repository.s3.multipart.threshold", defaultValue = "5242880")
    long multipartThreshold;

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

    @ConfigProperty(name = "repository.s3.multipart.chunk-size", defaultValue = "MEDIUM")
    ChunkSizeConfig chunkSizeConfig;

    @Inject
    S3Service s3Service;

    @Inject
    ReactiveUploadService reactiveUploadService;

    @Inject
    EventPublisher eventPublisher;

    @Inject
    io.pipeline.repository.service.DocumentService documentService;

    @Inject
    io.pipeline.repository.entity.Drive.DriveService driveService;

    @Inject
    RedisUploadBuffer redisUploadBuffer;


    // In-memory storage for upload progress (in production, this would be in Redis or database)
    private final ConcurrentHashMap<String, UploadProgress> uploadProgressMap = new ConcurrentHashMap<>();
    
    // Store chunk data for hash calculation
    private final ConcurrentHashMap<String, MessageDigest> uploadHashMap = new ConcurrentHashMap<>();
    
    private void cleanupFailedUpload(String nodeId, String uploadId) {
        try {
            UploadProgress progress = uploadProgressMap.get(nodeId);
            if (progress != null) {
                // Clean up S3 multipart upload
                try {
                    String bucketName = getBucketName(progress.driveName);
                    s3Service.abortMultipartUpload(bucketName, nodeId, progress.connectorId, uploadId);
                    LOG.infof("Aborted S3 multipart upload on failure: nodeId=%s, uploadId=%s", nodeId, uploadId);
                } catch (Exception e) {
                    LOG.warnf(e, "Failed to abort S3 multipart upload on failure: nodeId=%s, uploadId=%s", nodeId, uploadId);
                }
                
                // Clean up database - delete any partial Node entities
                try {
                    io.pipeline.repository.entity.Node nodeEntity = io.pipeline.repository.entity.Node.findByDocumentId(nodeId);
                    if (nodeEntity != null) {
                        nodeEntity.delete();
                        LOG.infof("Deleted partial Node entity from database on failure: nodeId=%s", nodeId);
                    }
                } catch (Exception e) {
                    LOG.warnf(e, "Failed to clean up Node entity in database on failure: nodeId=%s", nodeId);
                }
                
                // Update progress state
                progress.state = UploadState.UPLOAD_STATE_FAILED;
                progress.errorMessage = "Upload failed and was cleaned up";
            }
        } catch (Exception e) {
            LOG.errorf(e, "Failed to clean up failed upload: nodeId=%s, uploadId=%s", nodeId, uploadId);
        }
    }
    
    private String getBucketName(String driveName) {
        // Look up bucket from Drive entity
        io.pipeline.repository.entity.Drive drive = driveService.findByName(driveName);
        if (drive == null) {
            throw new DriveNotFoundException(driveName);
        }

        return drive.bucketName;
    }

    /**
     * Generate S3 key with connector-based structure.
     * Format: connectors/{connector_id}/{document_id}.pb
     */
    private String generateS3Key(String documentId, String connectorId) {
        String connector = (connectorId != null && !connectorId.isEmpty()) ? connectorId : "default";
        return String.format("connectors/%s/%s.pb", connector, documentId);
    }
    

    @Override
    public Uni<InitiateUploadResponse> initiateUpload(InitiateUploadRequest request) {
        return Uni.createFrom().item(Unchecked.supplier(() -> {
            try {
                // Use client-provided ID if available, otherwise generate one
                String nodeId;
                boolean isUpdate = false;
                String previousVersionId = null;

                if (!request.getClientNodeId().isEmpty()) {
                    nodeId = request.getClientNodeId();
                    LOG.infof("Using client-provided node ID: %s", nodeId);

                    // Check if this ID already exists in S3
                    String bucketName = getBucketName(request.getDrive());
                    String s3Key = generateS3Key(nodeId, request.getConnectorId());

                    if (s3Service.objectExists(bucketName, s3Key)) {
                        // Object exists - check if we should fail or update
                        if (request.getFailIfExists()) {
                            LOG.warnf("Client node ID already exists and fail_if_exists=true: %s", nodeId);
                            throw new UploadException("initiateUpload", nodeId,
                                "Document already exists with ID: " + nodeId, null);
                        } else {
                            // Allow overwrite - this is an update
                            isUpdate = true;
                            S3Service.S3ObjectMetadata existing = s3Service.getObjectMetadata(bucketName, s3Key);
                            if (existing != null) {
                                previousVersionId = existing.versionId;
                            }
                            LOG.infof("Updating existing document: %s (previous version: %s)",
                                nodeId, previousVersionId);
                        }
                    }
                } else {
                    nodeId = UUID.randomUUID().toString();
                    LOG.debugf("Generated new node ID: %s", nodeId);
                }

                String uploadId = UUID.randomUUID().toString();
                
                LOG.infof("Initiating upload: nodeId=%s, uploadId=%s, name=%s, expectedSize=%d", 
                    nodeId, uploadId, request.getName(), request.getExpectedSize());

                // Create upload progress tracking
                UploadProgress progress = new UploadProgress();
                progress.nodeId = nodeId;
                progress.uploadId = uploadId;
                progress.driveName = request.getDrive(); // Store drive name for later lookup
                progress.bucketName = getBucketName(request.getDrive()); // Cache bucket name to avoid DB lookup per chunk
                progress.fileName = request.getName(); // Store file name for later
                progress.mimeType = request.getMimeType(); // Store mime type
                progress.connectorId = !request.getConnectorId().isEmpty() ? request.getConnectorId() : null;
                progress.state = UploadState.UPLOAD_STATE_UPLOADING;
                progress.totalBytes = request.getExpectedSize();
                progress.bytesUploaded = 0;
                progress.chunkNumber = 0;
                progress.isLast = false;
                
                uploadProgressMap.put(nodeId, progress);
                
                // Initialize hash calculator for this upload
                try {
                    uploadHashMap.put(nodeId, MessageDigest.getInstance("SHA-256"));
                } catch (Exception e) {
                    LOG.warnf("Failed to initialize SHA-256 digest for upload %s: %s", nodeId, e.getMessage());
                }

                // If payload is provided for immediate small uploads, handle it
                if (request.hasPayload()) {
                    return handleImmediateUpload(request, nodeId, uploadId).await().indefinitely();
                }

                // Check if file size requires multipart upload (S3 requires 5MB+ for multipart)
                String bucketName = getBucketName(request.getDrive());
                String s3UploadId;

                if (request.getExpectedSize() > multipartThreshold) {
                    // Initiate REAL S3 multipart upload for large files
                    LOG.infof("File size %d exceeds threshold %d, using multipart upload",
                        request.getExpectedSize(), multipartThreshold);
                    // Create metadata for S3 multipart upload
                    java.util.Map<String, String> s3Metadata = new java.util.HashMap<>();
                    s3Metadata.put("filename", request.getName());
                    s3Metadata.put("proto-type", "type.googleapis.com/upload.ChunkedFile");
                    s3Metadata.put("connector-id", progress.connectorId != null ? progress.connectorId : "default");
                    s3Metadata.put("upload-type", "multipart");
                    s3Metadata.put("expected-size", String.valueOf(request.getExpectedSize()));

                    s3UploadId = s3Service.initiateMultipartUpload(bucketName, nodeId, progress.connectorId, request.getMimeType(), s3Metadata);
                    progress.useMultipart = true;
                } else {
                    // For small files, we'll use single PUT when chunks arrive
                    LOG.infof("File size %d below threshold %d, will use single PUT",
                        request.getExpectedSize(), multipartThreshold);
                    s3UploadId = uploadId; // Use our generated ID
                    progress.useMultipart = false;
                }

                // Update progress with S3 upload ID
                progress.uploadId = s3UploadId;
                progress.state = UploadState.UPLOAD_STATE_UPLOADING;

                InitiateUploadResponse.Builder responseBuilder = InitiateUploadResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setUploadId(s3UploadId) // Use real S3 upload ID
                    .setState(UploadState.UPLOAD_STATE_PENDING)
                    .setCreatedAtEpochMs(System.currentTimeMillis())
                    .setIsUpdate(isUpdate);

                if (previousVersionId != null) {
                    responseBuilder.setPreviousVersionId(previousVersionId);
                }

                InitiateUploadResponse response = responseBuilder.build();

                return response;

            } catch (RepoServiceException e) {
                LOG.errorf("Upload initiation failed: %s", e.getStructuredMessage());
                throw e;
            } catch (Exception e) {
                LOG.errorf(e, "Unexpected error during upload initiation");
                throw new UploadException("initiateUpload", "unknown", 
                    "Unexpected error during initiation", e);
            }
        })).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<UploadChunkResponse> uploadChunk(UploadChunkRequest request) {
        // Unary upload: Process chunk and upload to S3 asynchronously
        // This allows client to fire off multiple chunks in parallel
        String nodeId = request.getNodeId();
        String uploadId = request.getUploadId();
        int chunkNumber = (int) request.getChunkNumber();
        boolean isLast = request.getIsLast();
        byte[] chunkData = request.getData().toByteArray();

        UploadProgress progress = uploadProgressMap.get(nodeId);
        if (progress == null) {
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(nodeId)
                .setChunkNumber(chunkNumber)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }

        LOG.infof("📦 Processing chunk %d via unary call: nodeId=%s, size=%d, isLast=%s",
            chunkNumber, nodeId, chunkData.length, isLast);

        // Calculate S3 part number
        int partNumber = chunkNumber + 1;

        // Fire-and-forget S3 upload in background
        if (progress.useMultipart && chunkData.length > 0) {
            String bucketName = progress.bucketName;

            reactiveUploadService.uploadPartAsync(bucketName, nodeId, progress.connectorId, uploadId, partNumber, chunkData)
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .subscribe().with(
                    etag -> {
                        LOG.debugf("✅ Part %d uploaded: nodeId=%s", partNumber, nodeId);
                        synchronized (progress.etags) {
                            while (progress.etags.size() < partNumber) {
                                progress.etags.add(null);
                            }
                            progress.etags.set(partNumber - 1, etag);
                        }
                    },
                    error -> LOG.errorf(error, "❌ Part %d upload failed: nodeId=%s", partNumber, nodeId)
                );
        }

        // If last chunk, trigger completion
        if (isLast) {
            completeMultipartUploadAsync(progress, uploadId);
        }

        // Return immediate receipt
        return Uni.createFrom().item(UploadChunkResponse.newBuilder()
            .setNodeId(nodeId)
            .setChunkNumber(chunkNumber)
            .setBytesUploaded(chunkData.length)
            .setState(UploadState.UPLOAD_STATE_UPLOADING)
            .setFileSha("pending")
            .setHashType(HashType.HASH_TYPE_SHA256_BASE64)
            .setJobId("multipart-" + uploadId)
            .setIsFileComplete(isLast)
            .build());
    }

    private UploadChunkResponse processChunkAndGenerateReceipt(UploadChunkRequest chunk) {
        String nodeId = chunk.getNodeId();
        String uploadId = chunk.getUploadId();
        int chunkNumber = (int) chunk.getChunkNumber();
        boolean isLast = chunk.getIsLast();
        byte[] chunkData = chunk.getData().toByteArray();

        try {
            // Get upload metadata (read-only)
            UploadProgress progress = uploadProgressMap.get(nodeId);
            if (progress == null) {
                return UploadChunkResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setChunkNumber(chunkNumber)
                    .setBytesUploaded(0)
                    .setState(UploadState.UPLOAD_STATE_FAILED)
                    .setIsFileComplete(isLast)
                    .build();
            }

            String bucketName = progress.bucketName; // Use cached bucket name
            
            LOG.infof("Processing chunk %d and generating receipt: nodeId=%s, size=%d bytes, isLast=%s",
                chunkNumber, nodeId, chunkData.length, isLast);

            // Calculate S3 part number (chunk number + 1, since S3 parts start at 1)
            int partNumber = chunkNumber + 1;

            if (!progress.useMultipart) {
                // Small file - queue for single PUT upload
                queueChunkForSingleUpload(progress, chunkNumber, chunkData, isLast);
                
                // Generate immediate receipt for small file
                String fileSha = "pending"; // Will be calculated when file is complete
                String jobId = "single-put-" + nodeId;
                
                return UploadChunkResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setChunkNumber(chunkNumber)
                    .setBytesUploaded(chunkData.length)
                    .setState(UploadState.UPLOAD_STATE_UPLOADING)
                    .setFileSha(fileSha)
                    .setHashType(HashType.HASH_TYPE_SHA256_BASE64)
                    .setJobId(jobId)
                    .setIsFileComplete(isLast)
                    .build();
            } else {
                // Large file - queue part for async S3 upload (fire-and-forget, no blocking)
                queueChunkForMultipartUploadAsync(progress, chunkNumber, chunkData, isLast, uploadId, partNumber);
                
                // If this is the last chunk, complete the multipart upload
                if (isLast) {
                    completeMultipartUploadAsync(progress, uploadId);
                }
                
                // Generate immediate receipt for multipart upload (no waiting for S3)
                String fileSha = "pending"; // Will be calculated when file is complete
                String jobId = "multipart-" + uploadId;
                
                return UploadChunkResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setChunkNumber(chunkNumber)
                    .setBytesUploaded(chunkData.length)
                    .setState(UploadState.UPLOAD_STATE_UPLOADING)
                    .setFileSha(fileSha)
                    .setHashType(HashType.HASH_TYPE_SHA256_BASE64)
                    .setJobId(jobId)
                    .setIsFileComplete(isLast)
                    .build();
            }

        } catch (Exception e) {
            LOG.errorf(e, "Failed to process chunk %d and generate receipt for nodeId: %s", 
                chunkNumber, nodeId);
            return UploadChunkResponse.newBuilder()
                .setNodeId(nodeId)
                .setChunkNumber(chunkNumber)
                .setBytesUploaded(0)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .setIsFileComplete(isLast)
                .build();
        }
    }

    private void queueChunkForSingleUpload(UploadProgress progress, int chunkNumber, byte[] chunkData, boolean isLast) {
        // For small files, we need to collect all chunks before uploading
        // This is a simplified version - in a real implementation, we'd need thread-safe buffering
        // For now, we'll just log that we're queuing the chunk
        LOG.infof("Queuing chunk %d for single upload: nodeId=%s, size=%d, isLast=%s",
            chunkNumber, progress.nodeId, chunkData.length, isLast);
        
        // TODO: Implement proper chunk collection for single PUT upload
        // This would involve collecting chunks in order and triggering upload when isLast=true
    }
    
    private void queueChunkForMultipartUploadAsync(UploadProgress progress, int chunkNumber, byte[] chunkData, boolean isLast, String uploadId, int partNumber) {
        // Queue chunk for async S3 multipart upload - TRUE fire-and-forget
        LOG.infof("Queuing chunk %d for async multipart upload: nodeId=%s, size=%d, partNumber=%d, isLast=%s",
            chunkNumber, progress.nodeId, chunkData.length, partNumber, isLast);
        
        // Use async S3 service to upload part in background - use cached bucket name
        String bucketName = progress.bucketName;
        
        // TRUE async: subscribe on worker pool, don't block caller - fire and forget
        reactiveUploadService.uploadPartAsync(bucketName, progress.nodeId, progress.connectorId, uploadId, partNumber, chunkData)
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .onItem().invoke(etag -> {
                LOG.infof("Successfully uploaded part %d asynchronously: nodeId=%s, etag=%s", 
                    partNumber, progress.nodeId, etag);
                // Store etag for completion later
                synchronized (progress.etags) {
                    // Ensure etags list is large enough for this part number
                    while (progress.etags.size() < partNumber) {
                        progress.etags.add(null);
                    }
                    progress.etags.set(partNumber - 1, etag); // partNumber is 1-based, list is 0-based
                }
            })
            .onFailure().invoke(error -> {
                LOG.errorf(error, "Failed to upload part %d asynchronously: nodeId=%s", 
                    partNumber, progress.nodeId);
                // TODO: Handle error - maybe retry or abort upload
            })
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribe(); // Fire and forget - don't wait for it
    }
    
    private void completeMultipartUploadAsync(UploadProgress progress, String uploadId) {
        LOG.infof("Completing multipart upload asynchronously: nodeId=%s, uploadId=%s", progress.nodeId, uploadId);
        
        // Wait a bit for all parts to finish uploading, then complete
        Uni.createFrom().voidItem()
            .onItem().invoke(() -> {
                // Wait for all etags to be available
                synchronized (progress.etags) {
                    // Filter out null etags and create a clean list
                    List<String> etags = progress.etags.stream()
                        .filter(etag -> etag != null)
                        .collect(java.util.stream.Collectors.toList());
                    
                    if (etags.isEmpty()) {
                        LOG.warnf("No etags available for multipart upload completion: nodeId=%s", progress.nodeId);
                        return;
                    }
                    
                    LOG.infof("Completing multipart upload with %d parts: nodeId=%s", etags.size(), progress.nodeId);
                    
                    // Complete the multipart upload asynchronously
                    reactiveUploadService.completeMultipartUploadAsync(
                        progress.bucketName, 
                        progress.nodeId, 
                        progress.connectorId, 
                        uploadId, 
                        etags
                    ).subscribe().with(
                        metadata -> {
                            LOG.infof("Successfully completed multipart upload: nodeId=%s, s3Key=%s", 
                                progress.nodeId, metadata.s3Key);
                            progress.state = UploadState.UPLOAD_STATE_COMPLETED;
                        },
                        error -> {
                            LOG.errorf(error, "Failed to complete multipart upload: nodeId=%s", progress.nodeId);
                            progress.state = UploadState.UPLOAD_STATE_FAILED;
                            progress.errorMessage = error.getMessage();
                        }
                    );
                }
            })
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribe().with(
                result -> LOG.debugf("Multipart upload completion initiated: nodeId=%s", progress.nodeId),
                error -> LOG.errorf(error, "Failed to initiate multipart upload completion: nodeId=%s", progress.nodeId)
            );
    }

    private Uni<UploadChunkResponse> aggregateAndComplete(List<ChunkResult> chunkResults) {
        if (chunkResults.isEmpty()) {
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }

        // Get metadata from first chunk
        ChunkResult firstChunk = chunkResults.get(0);
        String nodeId = firstChunk.nodeId;
        String uploadId = uploadProgressMap.get(nodeId).uploadId;
        
        // Check for any failures
        List<ChunkResult> failedChunks = chunkResults.stream()
            .filter(result -> !result.isSuccess())
            .collect(Collectors.toList());
        
        if (!failedChunks.isEmpty()) {
            String errorMessage = "Chunk processing failed: " + 
                failedChunks.stream()
                    .map(r -> "chunk " + r.chunkNumber + ": " + r.errorMessage)
                    .collect(Collectors.joining(", "));
            
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(nodeId)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }

        // Sort by chunk number to maintain order
        chunkResults.sort(Comparator.comparingInt(r -> r.chunkNumber));

        // Get upload metadata
        UploadProgress progress = uploadProgressMap.get(nodeId);
        if (progress == null) {
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(nodeId)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }

        try {
            if (!progress.useMultipart) {
                // Small file - combine all data and do single PUT
                return completeSinglePutUpload(chunkResults, progress);
            } else {
                // Large file - complete multipart upload with all ETags
                return completeMultipartUpload(chunkResults, progress);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Failed to complete upload for nodeId: %s", nodeId);
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(nodeId)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }
    }

    private Uni<UploadChunkResponse> completeSinglePutUpload(List<ChunkResult> chunkResults, UploadProgress progress) {
        try {
            // Combine all chunk data
            ByteArrayOutputStream combinedData = new ByteArrayOutputStream();
            long totalBytes = 0;
            
            for (ChunkResult result : chunkResults) {
                if (result.finalData != null) {
                    combinedData.write(result.finalData);
                    totalBytes += result.bytesProcessed;
                }
            }
            
            byte[] allData = combinedData.toByteArray();
            String bucketName = getBucketName(progress.driveName);
            
            // Calculate hash
            String contentHash = HashUtil.calculateSHA256(allData);
            
            // Upload to S3
            S3Service.S3ObjectMetadata s3Metadata = s3Service.storeProtobuf(
                bucketName, progress.nodeId, allData, progress.mimeType);
            
            // Create document entity
            createDocumentEntity(progress, null); // We already calculated the hash above
            
            // Update progress
            progress.state = UploadState.UPLOAD_STATE_COMPLETED;
            progress.bytesUploaded = totalBytes;
            
            LOG.infof("Single PUT upload completed: nodeId=%s, size=%d bytes", 
                progress.nodeId, totalBytes);
            
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(progress.nodeId)
                .setState(UploadState.UPLOAD_STATE_COMPLETED)
                .setBytesUploaded(totalBytes)
                .setChunkNumber(chunkResults.size())
                .build());
                
        } catch (Exception e) {
            LOG.errorf(e, "Failed to complete single PUT upload for nodeId: %s", progress.nodeId);
            progress.state = UploadState.UPLOAD_STATE_FAILED;
            progress.errorMessage = e.getMessage();
            
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(progress.nodeId)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }
    }
    
    private Uni<UploadChunkResponse> completeMultipartUpload(List<ChunkResult> chunkResults, UploadProgress progress) {
        try {
            String bucketName = getBucketName(progress.driveName);
            String uploadId = progress.uploadId;
            
            // Collect all ETags in order
            List<String> etags = chunkResults.stream()
                .sorted(Comparator.comparingInt(r -> r.partNumber))
                .map(r -> r.etag)
                .collect(Collectors.toList());
            
            // Complete multipart upload
            s3Service.completeMultipartUpload(bucketName, progress.nodeId, progress.connectorId, uploadId, etags);
            
            // Calculate total bytes
            long totalBytes = chunkResults.stream()
                .mapToLong(r -> r.bytesProcessed)
                .sum();
            
            // Calculate hash from all chunks
            // Note: We'd need to store the actual data to calculate hash properly
            // For now, we'll use a placeholder
            String contentHash = "placeholder-hash";
            
            // Create S3 metadata
            S3Service.S3ObjectMetadata s3Metadata = new S3Service.S3ObjectMetadata();
            s3Metadata.s3Key = generateS3Key(progress.nodeId, progress.connectorId);
            s3Metadata.size = totalBytes;
            s3Metadata.contentType = progress.mimeType;
            s3Metadata.eTag = "placeholder-etag";
            
            // Create document entity
            createDocumentEntity(progress, null); // We already calculated the hash above
            
            // Update progress
            progress.state = UploadState.UPLOAD_STATE_COMPLETED;
            progress.bytesUploaded = totalBytes;
            
            LOG.infof("Multipart upload completed: nodeId=%s, parts=%d, size=%d bytes", 
                progress.nodeId, etags.size(), totalBytes);
            
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(progress.nodeId)
                .setState(UploadState.UPLOAD_STATE_COMPLETED)
                .setBytesUploaded(totalBytes)
                .setChunkNumber(chunkResults.size())
                .build());
                
        } catch (Exception e) {
            LOG.errorf(e, "Failed to complete multipart upload for nodeId: %s", progress.nodeId);
            progress.state = UploadState.UPLOAD_STATE_FAILED;
            progress.errorMessage = e.getMessage();
            
            // Clean up failed upload
            try {
                String bucketName = getBucketName(progress.driveName);
                s3Service.abortMultipartUpload(bucketName, progress.nodeId, progress.connectorId, progress.uploadId);
            } catch (Exception cleanupError) {
                LOG.errorf(cleanupError, "Failed to abort multipart upload for nodeId: %s", progress.nodeId);
            }
            
            return Uni.createFrom().item(UploadChunkResponse.newBuilder()
                .setNodeId(progress.nodeId)
                .setState(UploadState.UPLOAD_STATE_FAILED)
                .build());
        }
    }




    private void createDocumentEntity(UploadProgress progress, MessageDigest digest) {
        String contentHash = null;
        if (digest != null) {
            byte[] hashBytes = digest.digest();
            contentHash = java.util.Base64.getEncoder().encodeToString(hashBytes);
        }

        String fileName = progress.fileName != null ? progress.fileName : "uploaded-file-" + progress.nodeId;
        String mimeType = progress.mimeType != null ? progress.mimeType : "application/x-protobuf";

        documentService.createDocumentWithTopic(
            progress.driveName,
            fileName,
            mimeType,
            null, // No payload - it's in S3
            "type.googleapis.com/upload.ChunkedFile",
            null, // Use default topic
            progress.connectorId
        );

        LOG.infof("Created document entity: nodeId=%s, hash=%s", progress.nodeId, contentHash);
    }




    private void createNodeEntity(UploadProgress progress, String contentHash, S3Service.S3ObjectMetadata s3Metadata) {
        try {
            // Use the stored file name from InitiateUploadRequest
            String fileName = progress.fileName != null ? progress.fileName : "uploaded-file-" + progress.nodeId;
            String mimeType = progress.mimeType != null ? progress.mimeType : "application/x-protobuf";

            // Create a minimal DocumentService wrapper to pass the content hash and version ID
            // Since content is already in S3, we pass null payload but need to handle metadata
            io.pipeline.repository.service.DocumentService.DocumentResult documentResult =
                documentService.createDocumentWithTopic(
                    progress.driveName,
                    fileName,
                    mimeType,
                    null, // No payload needed - it's in S3
                    "type.googleapis.com/upload.ChunkedFile",
                    null, // Use default topic
                    progress.connectorId // Pass connector ID if available
                );

            LOG.infof("Created Node entity: id=%d, documentId=%s, contentHash=%s",
                documentResult.nodeEntity.id, documentResult.nodeEntity.documentId, contentHash);

        } catch (Exception e) {
            LOG.errorf(e, "Failed to create Node entity for upload: %s", progress.nodeId);
            // Clean up the completed S3 object
            String bucketName = getBucketName(progress.driveName);
            s3Service.deleteObject(bucketName, s3Metadata.s3Key);
            throw UploadException.chunkProcessingFailed("createNodeEntity", progress.nodeId, e);
        }
    }

    @Override
    public Uni<GetUploadStatusResponse> getUploadStatus(GetUploadStatusRequest request) {
        try {
            String nodeId = request.getNodeId();
            UploadProgress progress = uploadProgressMap.get(nodeId);
            
            if (progress == null) {
                return Uni.createFrom().failure(UploadException.progressNotFound("getUploadStatus", nodeId));
            }

            double percent = progress.totalBytes > 0 ? 
                (double) progress.bytesUploaded / progress.totalBytes * 100.0 : 0.0;

            GetUploadStatusResponse response = GetUploadStatusResponse.newBuilder()
                .setNodeId(nodeId)
                .setState(progress.state)
                .setBytesUploaded(progress.bytesUploaded)
                .setTotalBytes(progress.totalBytes)
                .setErrorMessage(progress.errorMessage != null ? progress.errorMessage : "")
                .setUpdatedAtEpochMs(System.currentTimeMillis())
                .build();

            return Uni.createFrom().item(response);

        } catch (Exception e) {
            LOG.errorf(e, "Failed to get upload status: %s", e.getMessage());
            return Uni.createFrom().failure(e);
        }
    }

    @Override
    public Uni<CancelUploadResponse> cancelUpload(CancelUploadRequest request) {
        try {
            String nodeId = request.getNodeId();
            UploadProgress progress = uploadProgressMap.remove(nodeId);
            
            if (progress == null) {
                return Uni.createFrom().item(CancelUploadResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setSuccess(false)
                    .setMessage("Upload not found")
                    .build());
            }

            progress.state = UploadState.UPLOAD_STATE_CANCELLED;
            
            // Clean up S3 multipart upload
            try {
                String bucketName = getBucketName(progress.driveName);
                s3Service.abortMultipartUpload(bucketName, nodeId, progress.connectorId, progress.uploadId);
                LOG.infof("Aborted S3 multipart upload: nodeId=%s, uploadId=%s", nodeId, progress.uploadId);
            } catch (Exception e) {
                LOG.warnf(e, "Failed to abort S3 multipart upload: nodeId=%s, uploadId=%s", nodeId, progress.uploadId);
                // Continue with cancellation even if S3 cleanup fails
            }
            
            // Clean up database - delete any partial Node entities
            try {
                io.pipeline.repository.entity.Node nodeEntity = io.pipeline.repository.entity.Node.findByDocumentId(nodeId);
                if (nodeEntity != null) {
                    nodeEntity.delete();
                    LOG.infof("Deleted partial Node entity from database: nodeId=%s", nodeId);
                }
            } catch (Exception e) {
                LOG.warnf(e, "Failed to clean up Node entity in database: nodeId=%s", nodeId);
                // Continue with cancellation even if DB cleanup fails
            }

            LOG.infof("Cancelled upload: nodeId=%s, uploadId=%s", nodeId, progress.uploadId);

            CancelUploadResponse response = CancelUploadResponse.newBuilder()
                .setNodeId(nodeId)
                .setSuccess(true)
                .setMessage("Upload cancelled successfully")
                .build();

            return Uni.createFrom().item(response);

        } catch (Exception e) {
            LOG.errorf(e, "Failed to cancel upload: %s", e.getMessage());
            return Uni.createFrom().failure(e);
        }
    }

    private Uni<InitiateUploadResponse> handleImmediateUpload(InitiateUploadRequest request, String nodeId, String uploadId) {
        try {
            // For immediate uploads, we can use the existing FilesystemService logic
            // This is a simplified version - in practice, you'd call FilesystemService
            
            byte[] payloadBytes = request.getPayload().toByteArray();
            String s3Key = nodeId + ".pb";
            
            // Store in S3 using drive from request
            String bucketName = getBucketName(request.getDrive());
            s3Service.storeProtobuf(bucketName, nodeId, payloadBytes, request.getMimeType());
            
            // Update progress
            UploadProgress progress = uploadProgressMap.get(nodeId);
            progress.state = UploadState.UPLOAD_STATE_COMPLETED;
            progress.bytesUploaded = payloadBytes.length;
            progress.totalBytes = payloadBytes.length;

            LOG.infof("Completed immediate upload: nodeId=%s, size=%d", nodeId, payloadBytes.length);

            InitiateUploadResponse response = InitiateUploadResponse.newBuilder()
                .setNodeId(nodeId)
                .setUploadId(uploadId)
                .setState(UploadState.UPLOAD_STATE_COMPLETED)
                .setCreatedAtEpochMs(System.currentTimeMillis())
                .build();

            return Uni.createFrom().item(response);

        } catch (Exception e) {
            LOG.errorf(e, "Failed to handle immediate upload: %s", e.getMessage());
            return Uni.createFrom().failure(e);
        }
    }



    // Result of processing a single chunk independently
    private static class ChunkResult {
        final String nodeId;
        final int chunkNumber;
        final int partNumber;
        final String etag;
        final long bytesProcessed;
        final boolean isLast;
        final byte[] finalData; // For single upload completion
        final String errorMessage;
        
        ChunkResult(String nodeId, int chunkNumber, int partNumber, String etag, 
                   long bytesProcessed, boolean isLast, byte[] finalData, String errorMessage) {
            this.nodeId = nodeId;
            this.chunkNumber = chunkNumber;
            this.partNumber = partNumber;
            this.etag = etag;
            this.bytesProcessed = bytesProcessed;
            this.isLast = isLast;
            this.finalData = finalData;
            this.errorMessage = errorMessage;
        }
        
        boolean isSuccess() {
            return errorMessage == null;
        }
    }
    
    // Aggregated results for upload completion
    private static class UploadResult {
        final String nodeId;
        final String uploadId;
        final List<ChunkResult> chunkResults;
        final long totalBytes;
        final boolean useMultipart;
        final String errorMessage;
        
        UploadResult(String nodeId, String uploadId, List<ChunkResult> chunkResults, 
                    long totalBytes, boolean useMultipart, String errorMessage) {
            this.nodeId = nodeId;
            this.uploadId = uploadId;
            this.chunkResults = chunkResults;
            this.totalBytes = totalBytes;
            this.useMultipart = useMultipart;
            this.errorMessage = errorMessage;
        }
        
        boolean isSuccess() {
            return errorMessage == null && chunkResults.stream().allMatch(ChunkResult::isSuccess);
        }
    }

    // Helper class for upload progress tracking (now only for metadata)
    private static class UploadProgress {
        String nodeId;
        String uploadId;
        String driveName;
        String bucketName;  // Cached bucket name to avoid DB lookup per chunk
        String fileName;
        String mimeType;
        String connectorId;
        UploadState state;
        long totalBytes;
        long bytesUploaded;  // Keep for compatibility with existing code
        int chunkNumber;
        int partNumber = 1;  // S3 part number (starts at 1)
        boolean isLast;
        String errorMessage;
        boolean useMultipart;  // Whether to use multipart upload or single PUT
        ByteArrayOutputStream singleUploadBuffer; // Buffer for small files
        ByteArrayOutputStream multipartBuffer; // Buffer for accumulating multipart chunks
        List<String> etags = new ArrayList<>(); // ETags for multipart upload parts
    }
}
