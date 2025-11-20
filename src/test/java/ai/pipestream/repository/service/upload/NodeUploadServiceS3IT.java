package ai.pipestream.repository.service.upload;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.repository.filesystem.upload.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.UploadResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for NodeUploadService with real S3 (MinIO).
 * <p> 
 * This test verifies that:
 * 1. PipeDoc can be serialized and uploaded to S3
 * 2. S3 key generation follows the expected pattern: account/connector/path/docId-filename
 * 3. The service returns proper UploadResponse with S3 metadata
 * 4. Large messages can be uploaded successfully
 * <p> 
 * Prerequisites:
 * - MinIO running on localhost:9010 (test profile)
 * - Bucket "pipeline-documents" must exist (or be created by test)
 */
@QuarkusTest
class NodeUploadServiceS3IT {

    @InjectMock
    @GrpcClient("node-upload-service")
    NodeUploadServiceGrpc.NodeUploadServiceBlockingStub uploadService;

    @Inject
    S3AsyncClient s3Client;

    private static final String TEST_BUCKET = "pipeline-documents";

    @BeforeEach
    void setUp() {
        // Ensure bucket exists
        ensureBucketExists();
    }

    private void ensureBucketExists() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(TEST_BUCKET)
                    .build()).join();
        } catch (Exception e) {
            // Create bucket if it doesn't exist or any error occurs
            try {
                s3Client.createBucket(CreateBucketRequest.builder()
                        .bucket(TEST_BUCKET)
                        .build()).join();
            } catch (Exception createEx) {
                // Bucket might already exist, ignore
                System.out.println("Bucket creation result: " + createEx.getMessage());
            }
        }
    }

    @Test
    void uploadPipeDoc_smallFile_success() {
        // Arrange
        String testContent = "Hello, S3! This is a test file.";
        byte[] contentBytes = testContent.getBytes();

        PipeDoc pipeDoc = createTestPipeDoc(
                "test-account",
                "test-connector",
                "/test/path",
                "test-file.txt",
                "text/plain",
                contentBytes
        );

        // Act
        UploadResponse response = uploadService.uploadPipeDoc(pipeDoc);

        // Assert
        assertTrue(response.getSuccess(), "Upload should succeed");
        assertNotNull(response.getDocumentId(), "Document ID should be generated");
        assertNotNull(response.getS3Key(), "S3 key should be generated");
        assertEquals("Upload successful", response.getMessage());

        // Verify S3 key format: account/connector/path/docId-filename
        assertTrue(response.getS3Key().startsWith("test-account/test-connector/test/path/"),
                "S3 key should start with account/connector/path");
        assertTrue(response.getS3Key().endsWith("-test-file.txt"),
                "S3 key should end with -filename");

        // Verify file exists in S3 (we upload the serialized PipeDoc)
        verifyFileInS3(response.getS3Key(), pipeDoc.toByteArray().length);
    }

    @Test
    void uploadPipeDoc_largeFile_success() {
        // Arrange - Create a 1MB file
        int fileSize = 1024 * 1024; // 1MB
        byte[] largeContent = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            largeContent[i] = (byte) (i % 256);
        }

        PipeDoc pipeDoc = createTestPipeDoc(
                "test-account",
                "test-connector",
                "/large/files",
                "large-file.bin",
                "application/octet-stream",
                largeContent
        );

        // Act
        long startTime = System.nanoTime();
        UploadResponse response = uploadService.uploadPipeDoc(pipeDoc);
        long duration = System.nanoTime() - startTime;

        // Assert
        assertTrue(response.getSuccess(), "Large file upload should succeed");
        assertNotNull(response.getDocumentId());
        assertNotNull(response.getS3Key());

        // Verify file exists in S3 (we upload the serialized PipeDoc)
        verifyFileInS3(response.getS3Key(), pipeDoc.toByteArray().length);

        // Log performance
        double seconds = duration / 1_000_000_000.0;
        double mbps = (fileSize / (1024.0 * 1024.0)) / seconds;
        System.out.printf("Uploaded %d bytes in %.3f seconds (%.2f MB/s)%n", 
                fileSize, seconds, mbps);
    }

    @Test
    void uploadPipeDoc_withMetadata_success() {
        // Arrange
        byte[] content = "Test content with metadata".getBytes();
        
        SearchMetadata metadata = SearchMetadata.newBuilder()
                .putMetadata("connector_id", "meta-connector")
                .putMetadata("account_id", "meta-account")
                .putMetadata("custom_field", "custom_value")
                .setSourcePath("/metadata/test")
                .setCreationDate(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .build();

        Blob blob = Blob.newBuilder()
                .setData(ByteString.copyFrom(content))
                .setFilename("metadata-test.txt")
                .setMimeType("text/plain")
                .build();

        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setSearchMetadata(metadata)
                .setBlobBag(BlobBag.newBuilder().setBlob(blob).build())
                .build();

        // Act
        UploadResponse response = uploadService.uploadPipeDoc(pipeDoc);

        // Assert
        assertTrue(response.getSuccess());
        assertTrue(response.getS3Key().contains("meta-account/meta-connector"));
        
        // Verify metadata was preserved in S3
        verifyS3Metadata(response.getS3Key(), "custom_field", "custom_value");
    }

    @Test
    void uploadPipeDoc_rootPath_success() {
        // Arrange - Test with root path (empty or "/")
        PipeDoc pipeDoc = createTestPipeDoc(
                "root-account",
                "root-connector",
                "", // Empty path
                "root-file.txt",
                "text/plain",
                "Root file content".getBytes()
        );

        // Act
        UploadResponse response = uploadService.uploadPipeDoc(pipeDoc);

        // Assert
        assertTrue(response.getSuccess());
        assertTrue(response.getS3Key().contains("root-account/root-connector/root/"),
                "Empty path should be converted to 'root'");
    }

    // Helper methods

    private PipeDoc createTestPipeDoc(String accountId, String connectorId, 
                                     String path, String filename, String mimeType, 
                                     byte[] content) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        SearchMetadata metadata = SearchMetadata.newBuilder()
                .putMetadata("connector_id", connectorId)
                .putMetadata("account_id", accountId)
                .setSourcePath(path)
                .setCreationDate(timestamp)
                .setLastModifiedDate(timestamp)
                .build();

        Blob blob = Blob.newBuilder()
                .setData(ByteString.copyFrom(content))
                .setFilename(filename)
                .setMimeType(mimeType)
                .build();

        return PipeDoc.newBuilder()
                .setSearchMetadata(metadata)
                .setBlobBag(BlobBag.newBuilder().setBlob(blob).build())
                .build();
    }

    private void verifyFileInS3(String s3Key, int expectedSize) {
        try {
            HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(s3Key)
                    .build()).join();

            assertNotNull(headResponse, "File should exist in S3");
            assertEquals(expectedSize, headResponse.contentLength().intValue(),
                    "File size should match");
        } catch (Exception e) {
            fail("File should exist in S3: " + e.getMessage());
        }
    }

    private void verifyS3Metadata(String s3Key, String metadataKey, String expectedValue) {
        try {
            HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(s3Key)
                    .build()).join();

            String actualValue = headResponse.metadata().get(metadataKey);
            assertEquals(expectedValue, actualValue,
                    "S3 metadata should match");
        } catch (Exception e) {
            fail("Failed to verify S3 metadata: " + e.getMessage());
        }
    }
}

