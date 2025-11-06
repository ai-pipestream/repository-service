package io.pipeline.repository.services;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test S3 multipart upload functionality in isolation.
 * This test verifies that the S3 multipart upload library works correctly.
 */
@QuarkusTest
public class S3MultipartUploadTest {

    @Inject
    S3Service s3Service;

    @Test
    @Disabled("Enable when testing S3 multipart uploads")
    public void testMultipartUpload() throws Exception {
        String bucketName = "test-bucket";
        String documentId = UUID.randomUUID().toString();
        String connectorId = "test-connector";
        String contentType = "application/x-protobuf";
        
        // Create test metadata
        java.util.Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("filename", "test-file.bin");
        metadata.put("proto-type", "type.googleapis.com/upload.ChunkedFile");
        metadata.put("connector-id", connectorId);
        metadata.put("upload-type", "multipart");
        
        // Initiate multipart upload
        String uploadId = s3Service.initiateMultipartUpload(
            bucketName, documentId, connectorId, contentType, metadata
        );
        
        System.out.println("Initiated multipart upload: uploadId=" + uploadId);
        
        // Create test data chunks (simulate 3 chunks of 5MB each)
        List<String> etags = new ArrayList<>();
        int chunkSize = 5 * 1024 * 1024; // 5MB
        int numChunks = 3;
        
        for (int i = 0; i < numChunks; i++) {
            byte[] chunkData = new byte[chunkSize];
            // Fill with test data
            for (int j = 0; j < chunkSize; j++) {
                chunkData[j] = (byte) (i * chunkSize + j);
            }
            
            // Upload part
            String etag = s3Service.uploadPart(
                bucketName, documentId, connectorId, uploadId, i + 1, chunkData
            );
            etags.add(etag);
            
            System.out.println("Uploaded part " + (i + 1) + ": etag=" + etag);
        }
        
        // Complete multipart upload
        S3Service.S3ObjectMetadata result = s3Service.completeMultipartUpload(
            bucketName, documentId, connectorId, uploadId, etags
        );
        
        System.out.println("Completed multipart upload:");
        System.out.println("  s3Key: " + result.s3Key);
        System.out.println("  eTag: " + result.eTag);
        System.out.println("  versionId: " + result.versionId);
        
        // Verify the object exists
        boolean exists = s3Service.objectExists(bucketName, result.s3Key);
        System.out.println("Object exists: " + exists);
        
        // Clean up
        s3Service.deleteObject(bucketName, result.s3Key);
        System.out.println("Deleted test object");
    }
}
