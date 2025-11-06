package io.pipeline.repository.services;

public interface S3Service {

    String PROTOBUF_EXTENSION = ".pb";

    class S3ObjectMetadata {
        public String s3Key;
        public Long size;
        public String contentType;
        public String eTag;
        public String versionId;
        public java.time.Instant lastModified;
    }

    S3ObjectMetadata storeProtobuf(String bucketName, String documentId, byte[] data, String contentType);

    byte[] retrieveProtobuf(String bucketName, String documentId);

    // Retrieve object by full S3 key (for connector paths)
    byte[] getObject(String bucketName, String s3Key);

    boolean protobufExists(String bucketName, String documentId);

    // Check if object exists with full S3 key
    boolean objectExists(String bucketName, String s3Key);

    S3ObjectMetadata getProtobufMetadata(String bucketName, String documentId);

    // Get object metadata by full S3 key
    S3ObjectMetadata getObjectMetadata(String bucketName, String s3Key);

    void deleteProtobuf(String bucketName, String documentId);

    // Single PUT for small files with metadata
    S3ObjectMetadata putObject(String bucketName, String s3Key, byte[] data, String contentType,
                              java.util.Map<String, String> metadata);

    // S3 Multipart Upload methods for real chunked uploads with connector support
    String initiateMultipartUpload(String bucketName, String documentId, String connectorId, String contentType,
                                  java.util.Map<String, String> metadata);

    String uploadPart(String bucketName, String documentId, String connectorId, String uploadId, int partNumber, byte[] data);

    S3ObjectMetadata completeMultipartUpload(String bucketName, String documentId, String connectorId, String uploadId, java.util.List<String> etags);

    void abortMultipartUpload(String bucketName, String documentId, String connectorId, String uploadId);
    
    void deleteObject(String bucketName, String s3Key);
}
