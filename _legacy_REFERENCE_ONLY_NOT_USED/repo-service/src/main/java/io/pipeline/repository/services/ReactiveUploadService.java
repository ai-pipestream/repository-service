package io.pipeline.repository.services;

import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Map;

public interface ReactiveUploadService {
    Uni<S3Service.S3ObjectMetadata> storeProtobufAsync(String bucketName, String documentId, byte[] data, String contentType);

    Uni<byte[]> retrieveProtobufAsync(String bucketName, String documentId);

    Uni<Boolean> protobufExistsAsync(String bucketName, String documentId);

    Uni<Void> deleteProtobufAsync(String bucketName, String documentId);

    Uni<S3Service.S3ObjectMetadata> putObjectAsync(String bucketName, String s3Key, byte[] data, String contentType, Map<String, String> metadata);

    Uni<String> initiateMultipartUploadAsync(String bucketName, String documentId, String connectorId, String contentType, Map<String, String> metadata);

    Uni<String> uploadPartAsync(String bucketName, String documentId, String connectorId, String uploadId, int partNumber, byte[] data);

    Uni<S3Service.S3ObjectMetadata> completeMultipartUploadAsync(String bucketName, String documentId, String connectorId, String uploadId, List<String> etags);

    Uni<Void> abortMultipartUploadAsync(String bucketName, String documentId, String connectorId, String uploadId);
}
