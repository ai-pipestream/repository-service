package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.upload.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import com.google.protobuf.ByteString;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;

/**
 * REAL integration test for chunked uploads to S3.
 * This test proves that multipart uploads actually work with real S3.
 */
@QuarkusTest
public class ChunkedUploadIntegrationTest {

    @GrpcClient
    MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub uploadService;
    
    @Inject
    S3Service s3Service;
    
    private static final String TEST_DRIVE = "test-drive";
    private static final String TEST_BUCKET = "test-bucket";
    
    @BeforeEach
    void setup() {
        // Ensure test drive exists in database
        // This would be set up by test fixtures
    }
    
    @Test
    void testRealMultipartUploadToS3() {
        // Step 1: Initiate the upload
        InitiateUploadRequest initiateRequest = InitiateUploadRequest.newBuilder()
            .setDrive(TEST_DRIVE)
            .setName("large-test-file.bin")
            .setExpectedSize(10 * 1024 * 1024) // 10MB
            .setMimeType("application/octet-stream")
            .setConnectorId("test-connector")
            .build();
            
        InitiateUploadResponse initiateResponse = uploadService.initiateUpload(initiateRequest)
            .await().atMost(Duration.ofSeconds(10));
        
        assertNotNull(initiateResponse);
        assertNotNull(initiateResponse.getNodeId());
        assertNotNull(initiateResponse.getUploadId());
        assertEquals(UploadState.UPLOAD_STATE_PENDING, initiateResponse.getState());
        
        System.out.println("✅ Upload initiated: nodeId=" + initiateResponse.getNodeId() + 
                         ", uploadId=" + initiateResponse.getUploadId());
        
        // Step 2: Create chunks (5MB each)
        int chunkSize = 5 * 1024 * 1024; // 5MB per chunk
        List<UploadChunkRequest> chunks = new ArrayList<>();
        
        for (int i = 0; i < 2; i++) {
            byte[] chunkData = new byte[chunkSize];
            // Fill with test data (in real test, this would be actual file data)
            for (int j = 0; j < chunkData.length; j++) {
                chunkData[j] = (byte) (i + j % 256);
            }
            
            UploadChunkRequest chunk = UploadChunkRequest.newBuilder()
                .setNodeId(initiateResponse.getNodeId())
                .setUploadId(initiateResponse.getUploadId())
                .setChunkNumber(i + 1)
                .setData(ByteString.copyFrom(chunkData))
                .setIsLast(i == 1) // Last chunk
                .build();
                
            chunks.add(chunk);
            System.out.println("✅ Created chunk " + (i + 1) + ": " + chunkSize + " bytes");
        }
        
        // Step 3: Upload chunks
        List<UploadChunkResponse> uploadResponses = uploadService.uploadChunks(
            Multi.createFrom().items(chunks.stream())
        ).collect().asList().await().atMost(Duration.ofSeconds(30));
        
        // Get the last response (final state)
        UploadChunkResponse uploadResponse = uploadResponses.get(uploadResponses.size() - 1);
        
        assertNotNull(uploadResponse);
        assertEquals(UploadState.UPLOAD_STATE_COMPLETED, uploadResponse.getState());
        assertEquals(10 * 1024 * 1024, uploadResponse.getBytesUploaded());
        
        System.out.println("✅ Upload completed: " + uploadResponse.getBytesUploaded() + " bytes uploaded");
        
        // Step 4: Verify the file exists in S3
        String nodeId = initiateResponse.getNodeId();
        boolean exists = s3Service.protobufExists(TEST_BUCKET, nodeId);
        assertTrue(exists, "File should exist in S3");
        
        S3Service.S3ObjectMetadata metadata = s3Service.getProtobufMetadata(TEST_BUCKET, nodeId);
        assertNotNull(metadata);
        assertNotNull(metadata.eTag);
        assertNotNull(metadata.versionId);
        assertEquals(nodeId + ".pb", metadata.s3Key);
        
        System.out.println("✅ Verified in S3: key=" + metadata.s3Key + 
                         ", etag=" + metadata.eTag + 
                         ", versionId=" + metadata.versionId);
        
        // Step 5: Download and verify content
        byte[] downloadedData = s3Service.retrieveProtobuf(TEST_BUCKET, nodeId);
        assertNotNull(downloadedData);
        assertEquals(10 * 1024 * 1024, downloadedData.length);
        
        // Verify first few bytes match what we uploaded
        assertEquals((byte) 0, downloadedData[0]);
        assertEquals((byte) 1, downloadedData[1]);
        
        System.out.println("✅ Downloaded and verified: " + downloadedData.length + " bytes");
        
        // Cleanup
        s3Service.deleteProtobuf(TEST_BUCKET, nodeId);
        System.out.println("✅ Cleanup completed");
    }
    
    @Test
    void testUploadStatusTracking() {
        // Test that we can track upload progress
        InitiateUploadRequest initiateRequest = InitiateUploadRequest.newBuilder()
            .setDrive(TEST_DRIVE)
            .setName("status-test-file.bin")
            .setExpectedSize(1024)
            .setMimeType("application/octet-stream")
            .build();
            
        InitiateUploadResponse initiateResponse = uploadService.initiateUpload(initiateRequest)
            .await().atMost(Duration.ofSeconds(10));
        
        // Check status
        GetUploadStatusRequest statusRequest = GetUploadStatusRequest.newBuilder()
            .setNodeId(initiateResponse.getNodeId())
            .build();
            
        GetUploadStatusResponse statusResponse = uploadService.getUploadStatus(statusRequest)
            .await().atMost(Duration.ofSeconds(5));
            
        assertNotNull(statusResponse);
        assertEquals(UploadState.UPLOAD_STATE_PENDING, statusResponse.getState());
        assertEquals(0, statusResponse.getBytesUploaded());
        assertEquals(1024, statusResponse.getTotalBytes());
        
        System.out.println("✅ Upload status tracking works");
    }
    
    @Test 
    void testCancelUpload() {
        // Test that we can cancel an in-progress upload
        InitiateUploadRequest initiateRequest = InitiateUploadRequest.newBuilder()
            .setDrive(TEST_DRIVE)
            .setName("cancel-test-file.bin")
            .setExpectedSize(1024)
            .setMimeType("application/octet-stream")
            .build();
            
        InitiateUploadResponse initiateResponse = uploadService.initiateUpload(initiateRequest)
            .await().atMost(Duration.ofSeconds(10));
        
        // Cancel it
        CancelUploadRequest cancelRequest = CancelUploadRequest.newBuilder()
            .setNodeId(initiateResponse.getNodeId())
            .build();
            
        CancelUploadResponse cancelResponse = uploadService.cancelUpload(cancelRequest)
            .await().atMost(Duration.ofSeconds(5));
            
        assertNotNull(cancelResponse);
        assertTrue(cancelResponse.getSuccess());
        
        System.out.println("✅ Upload cancellation works");
    }
}