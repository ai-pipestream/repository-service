package io.pipeline.repository.services;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;

@ApplicationScoped
public class DefaultS3Service implements S3Service {

    private static final String DEFAULT_CONNECTOR = "default";

    @Inject
    S3Client s3Client; // Quarkus provides and manages this AWS SDK v2 client

    /**
     * Generate S3 key with connector-based structure.
     * Format: connectors/{connector_id}/{document_id}
     */
    private String generateS3Key(String documentId, String connectorId) {
        String connector = (connectorId != null && !connectorId.isEmpty()) ? connectorId : DEFAULT_CONNECTOR;
        return String.format("connectors/%s/%s", connector, documentId);
    }

    @Override
    public S3ObjectMetadata storeProtobuf(String bucketName, String documentId, byte[] data, String contentType) {
        String s3Key = generateS3Key(documentId, "default");
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType)
                .contentLength((long) data.length)
                .build();

        PutObjectResponse response = s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));

        S3ObjectMetadata metadata = new S3ObjectMetadata();
        metadata.s3Key = s3Key;
        metadata.size = (long) data.length;
        metadata.contentType = contentType;
        metadata.eTag = response.eTag();
        metadata.versionId = response.versionId();
        return metadata;
    }

    @Override
    public byte[] getObject(String bucketName, String s3Key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        try {
            return s3Client.getObject(getObjectRequest).readAllBytes();
        } catch (NoSuchKeyException e) {
            throw new RuntimeException("Object not found: " + s3Key, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve object: " + s3Key, e);
        }
    }

    @Override
    public byte[] retrieveProtobuf(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        try {
            return s3Client.getObject(getObjectRequest).readAllBytes();
        } catch (NoSuchKeyException e) {
            throw new RuntimeException("Protobuf not found: " + s3Key, e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve protobuf: " + s3Key, e);
        }
    }

    @Override
    public boolean protobufExists(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        return objectExists(bucketName, s3Key);
    }

    @Override
    public boolean objectExists(String bucketName, String s3Key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public S3ObjectMetadata getProtobufMetadata(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build());

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.s3Key = s3Key;
            metadata.size = response.contentLength();
            metadata.contentType = response.contentType();
            metadata.eTag = response.eTag();
            metadata.versionId = response.versionId();
            metadata.lastModified = response.lastModified();
            return metadata;
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String s3Key) {
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build());

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.s3Key = s3Key;
            metadata.size = response.contentLength();
            metadata.contentType = response.contentType();
            metadata.eTag = response.eTag();
            metadata.versionId = response.versionId();
            metadata.lastModified = response.lastModified();
            return metadata;
        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    @Override
    public void deleteProtobuf(String bucketName, String documentId) {
        String s3Key = documentId + PROTOBUF_EXTENSION;
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3Client.deleteObject(deleteObjectRequest);
    }

    @Override
    public S3ObjectMetadata putObject(String bucketName, String s3Key, byte[] data, String contentType,
                                     java.util.Map<String, String> metadata) {
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

        PutObjectResponse response = s3Client.putObject(putRequest, RequestBody.fromBytes(data));

        S3ObjectMetadata result = new S3ObjectMetadata();
        result.s3Key = s3Key;
        result.size = (long) data.length;
        result.contentType = contentType;
        result.eTag = response.eTag();
        result.versionId = response.versionId();
        return result;
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String documentId, String connectorId, String contentType,
                                         java.util.Map<String, String> metadata) {
        String s3Key = generateS3Key(documentId, connectorId);
        CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType);

        // Add metadata if provided
        if (metadata != null && !metadata.isEmpty()) {
            requestBuilder.metadata(metadata);
        }

        CreateMultipartUploadRequest createRequest = requestBuilder.build();
        
        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createRequest);
        return response.uploadId();
    }

    @Override
    public String uploadPart(String bucketName, String documentId, String connectorId, String uploadId, int partNumber, byte[] data) {
        String s3Key = generateS3Key(documentId, connectorId);
        UploadPartRequest uploadRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();
        
        RequestBody requestBody = RequestBody.fromBytes(data);
        UploadPartResponse response = s3Client.uploadPart(uploadRequest, requestBody);
        return response.eTag();
    }

    @Override
    public S3ObjectMetadata completeMultipartUpload(String bucketName, String documentId, String connectorId, String uploadId, java.util.List<String> etags) {
        String s3Key = generateS3Key(documentId, connectorId);
        
        // Build completed parts list
        java.util.List<CompletedPart> completedParts = new java.util.ArrayList<>();
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
        
        CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(completeRequest);
        
        // Return metadata
        S3ObjectMetadata metadata = new S3ObjectMetadata();
        metadata.s3Key = s3Key;
        metadata.eTag = response.eTag();
        metadata.versionId = response.versionId();
        return metadata;
    }

    @Override
    public void abortMultipartUpload(String bucketName, String documentId, String connectorId, String uploadId) {
        String s3Key = generateS3Key(documentId, connectorId);
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .uploadId(uploadId)
                .build();
        
        s3Client.abortMultipartUpload(abortRequest);
    }

    @Override
    public void deleteObject(String bucketName, String s3Key) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        
        s3Client.deleteObject(deleteRequest);
    }

}
