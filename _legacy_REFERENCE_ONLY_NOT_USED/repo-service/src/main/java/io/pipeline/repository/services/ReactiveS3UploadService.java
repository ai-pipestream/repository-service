package io.pipeline.repository.services;

import io.pipeline.repository.exception.S3StorageException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Reactive S3 service using AWS SDK v2 Async Client.
 * Returns Uni<T> for all operations instead of blocking.
 */
@ApplicationScoped
public class ReactiveS3UploadService implements ReactiveUploadService {
    
    private static final Logger LOG = Logger.getLogger(ReactiveS3UploadService.class);
    private static final String PROTOBUF_EXTENSION = ".pb";
    
    @Inject
    S3AsyncClient s3AsyncClient; // Quarkus provides this
    
    /**
     * Store a protobuf file in S3 reactively.
     */
    @Override
    public Uni<S3Service.S3ObjectMetadata> storeProtobufAsync(String bucketName, String documentId, byte[] data, String contentType) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        
        LOG.debugf("Storing protobuf reactively: bucket=%s, documentId=%s, size=%d", 
            bucketName, documentId, data.length);
        
        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .contentType(contentType)
            .contentLength((long) data.length)
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.putObject(putRequest, AsyncRequestBody.fromBytes(data)))
            .map(response -> {
                LOG.debugf("Successfully stored protobuf: %s (ETag: %s)", s3Key, response.eTag());
                
                S3Service.S3ObjectMetadata metadata = new S3Service.S3ObjectMetadata();
                metadata.s3Key = s3Key;
                metadata.size = (long) data.length;
                metadata.contentType = contentType;
                metadata.eTag = response.eTag();
                metadata.versionId = response.versionId();
                metadata.lastModified = Instant.now();
                return metadata;
            })
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to store protobuf: bucket=%s, documentId=%s", bucketName, documentId);
                return S3StorageException.uploadFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Retrieve a protobuf file from S3 reactively.
     */
    @Override
    public Uni<byte[]> retrieveProtobufAsync(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        
        GetObjectRequest getRequest = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.getObject(getRequest, software.amazon.awssdk.core.async.AsyncResponseTransformer.toBytes()))
            .map(response -> {
                byte[] data = response.asByteArray();
                LOG.debugf("Successfully retrieved protobuf: %s, size=%d", s3Key, data.length);
                return data;
            })
            .onFailure().transform(e -> {
                if (e.getCause() instanceof NoSuchKeyException) {
                    LOG.debugf("Protobuf not found: %s", s3Key);
                    return S3StorageException.downloadFailed(bucketName, s3Key, e);
                } else {
                    LOG.errorf(e, "Failed to retrieve protobuf: bucket=%s, documentId=%s", bucketName, documentId);
                    return S3StorageException.downloadFailed(bucketName, s3Key, e);
                }
            });
    }
    
    /**
     * Check if a protobuf exists in S3 reactively.
     */
    @Override
    public Uni<Boolean> protobufExistsAsync(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.headObject(headRequest))
            .map(response -> true)
            .onFailure(NoSuchKeyException.class).recoverWithItem(false)
            .onFailure().transform(e -> {
                if (!(e instanceof NoSuchKeyException)) {
                    LOG.errorf(e, "Failed to check protobuf existence: bucket=%s, documentId=%s", bucketName, documentId);
                    return S3StorageException.downloadFailed(bucketName, s3Key, e);
                }
                return e;
            });
    }
    
    /**
     * Delete a protobuf file from S3 reactively.
     */
    @Override
    public Uni<Void> deleteProtobufAsync(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.deleteObject(deleteRequest))
            .replaceWithVoid()
            .invoke(() -> LOG.debugf("Successfully deleted protobuf: %s", s3Key))
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to delete protobuf: bucket=%s, documentId=%s", bucketName, documentId);
                return S3StorageException.deleteFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Generate S3 key with connector-based structure.
     * Format: connectors/{connector_id}/{document_id}
     */
    private String generateS3Key(String documentId, String connectorId) {
        String connector = (connectorId != null && !connectorId.isEmpty()) ? connectorId : "default";
        return String.format("connectors/%s/%s", connector, documentId);
    }
    
    /**
     * Single PUT for small files with metadata (async).
     */
    @Override
    public Uni<S3Service.S3ObjectMetadata> putObjectAsync(String bucketName, String s3Key, byte[] data, String contentType, Map<String, String> metadata) {
        LOG.debugf("Storing object reactively: bucket=%s, key=%s, size=%d", bucketName, s3Key, data.length);
        
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .contentType(contentType)
            .contentLength((long) data.length);
        
        // Add metadata if provided
        if (metadata != null && !metadata.isEmpty()) {
            requestBuilder.metadata(metadata);
        }
        
        PutObjectRequest putRequest = requestBuilder.build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.putObject(putRequest, AsyncRequestBody.fromBytes(data)))
            .map(response -> {
                LOG.debugf("Successfully stored object: %s (ETag: %s)", s3Key, response.eTag());
                
                S3Service.S3ObjectMetadata result = new S3Service.S3ObjectMetadata();
                result.s3Key = s3Key;
                result.size = (long) data.length;
                result.contentType = contentType;
                result.eTag = response.eTag();
                result.versionId = response.versionId();
                result.lastModified = Instant.now();
                return result;
            })
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to store object: bucket=%s, key=%s", bucketName, s3Key);
                return S3StorageException.uploadFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Initiate multipart upload (async).
     */
    @Override
    public Uni<String> initiateMultipartUploadAsync(String bucketName, String documentId, String connectorId, String contentType, Map<String, String> metadata) {
        String s3Key = generateS3Key(documentId, connectorId);
        
        LOG.debugf("Initiating multipart upload: bucket=%s, key=%s", bucketName, s3Key);
        
        CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .contentType(contentType);
        
        // Add metadata if provided
        if (metadata != null && !metadata.isEmpty()) {
            requestBuilder.metadata(metadata);
        }
        
        CreateMultipartUploadRequest createRequest = requestBuilder.build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.createMultipartUpload(createRequest))
            .map(response -> {
                LOG.debugf("Successfully initiated multipart upload: %s (UploadId: %s)", s3Key, response.uploadId());
                return response.uploadId();
            })
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to initiate multipart upload: bucket=%s, key=%s", bucketName, s3Key);
                return S3StorageException.uploadFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Upload part (async).
     */
    @Override
    public Uni<String> uploadPartAsync(String bucketName, String documentId, String connectorId, String uploadId, int partNumber, byte[] data) {
        String s3Key = generateS3Key(documentId, connectorId);
        long startTime = System.currentTimeMillis();

        LOG.infof("⬆️  START S3 upload: partNumber=%d, size=%d bytes, thread=%s",
            partNumber, data.length, Thread.currentThread().getName());

        UploadPartRequest uploadRequest = UploadPartRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .build();

        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.uploadPart(uploadRequest, AsyncRequestBody.fromBytes(data)))
            .map(response -> {
                long duration = System.currentTimeMillis() - startTime;
                LOG.infof("✅ END S3 upload: partNumber=%d, duration=%dms, thread=%s (ETag: %s)",
                    partNumber, duration, Thread.currentThread().getName(), response.eTag());
                return response.eTag();
            })
            .onFailure().transform(e -> {
                long duration = System.currentTimeMillis() - startTime;
                LOG.errorf(e, "❌ FAILED S3 upload: partNumber=%d, duration=%dms, thread=%s",
                    partNumber, duration, Thread.currentThread().getName());
                return S3StorageException.uploadFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Complete multipart upload (async).
     */
    @Override
    public Uni<S3Service.S3ObjectMetadata> completeMultipartUploadAsync(String bucketName, String documentId, String connectorId, String uploadId, List<String> etags) {
        String s3Key = generateS3Key(documentId, connectorId);
        
        LOG.debugf("Completing multipart upload: bucket=%s, key=%s, parts=%d", bucketName, s3Key, etags.size());
        
        // Build completed parts list
        List<CompletedPart> completedParts = new java.util.ArrayList<>();
        for (int i = 0; i < etags.size(); i++) {
            completedParts.add(CompletedPart.builder()
                .partNumber(i + 1)
                .eTag(etags.get(i))
                .build());
        }
        
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build())
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.completeMultipartUpload(completeRequest))
            .map(response -> {
                LOG.debugf("Successfully completed multipart upload: %s (ETag: %s)", s3Key, response.eTag());
                
                S3Service.S3ObjectMetadata metadata = new S3Service.S3ObjectMetadata();
                metadata.s3Key = s3Key;
                metadata.eTag = response.eTag();
                metadata.versionId = response.versionId();
                metadata.lastModified = Instant.now();
                return metadata;
            })
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to complete multipart upload: bucket=%s, key=%s", bucketName, s3Key);
                return S3StorageException.uploadFailed(bucketName, s3Key, e);
            });
    }
    
    /**
     * Abort multipart upload (async).
     */
    @Override
    public Uni<Void> abortMultipartUploadAsync(String bucketName, String documentId, String connectorId, String uploadId) {
        String s3Key = generateS3Key(documentId, connectorId);
        
        LOG.debugf("Aborting multipart upload: bucket=%s, key=%s, uploadId=%s", bucketName, s3Key, uploadId);
        
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .uploadId(uploadId)
            .build();
        
        return Uni.createFrom()
            .completionStage(() -> s3AsyncClient.abortMultipartUpload(abortRequest))
            .replaceWithVoid()
            .invoke(() -> LOG.debugf("Successfully aborted multipart upload: %s", s3Key))
            .onFailure().transform(e -> {
                LOG.errorf(e, "Failed to abort multipart upload: bucket=%s, key=%s", bucketName, s3Key);
                return S3StorageException.deleteFailed(bucketName, s3Key, e);
            });
    }
}
