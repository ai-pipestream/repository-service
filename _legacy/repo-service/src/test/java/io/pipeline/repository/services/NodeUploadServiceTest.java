package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.upload.*;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EntityTestHelper;
import io.pipeline.repository.entity.Drive;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.Any;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class NodeUploadServiceTest {
    
    private static final Logger LOG = Logger.getLogger(NodeUploadServiceTest.class);
    
    @Inject
    @io.quarkus.grpc.GrpcService
    NodeUploadServiceImpl nodeUploadService;
    
    @Inject
    TestBucketManager bucketManager;
    
    @Inject
    EntityTestHelper entityHelper;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        String testBucket = bucketManager.setupBucket(testInfo);
        // No more setTestBucket() - service uses proper drive lookup!
    }
    
    @AfterEach
    void tearDown() {
        bucketManager.cleanupBucket(bucketManager.getCurrentBucket());
    }
    
    @Test
    @TestTransaction
    void testSimpleChunkedUpload() {
        LOG.info("Starting simple chunked upload test");
        
        // Create test drive for upload
        Drive testDrive = entityHelper.createTestDrive("upload-test", bucketManager.getCurrentBucket());
        
        // Step 1: Initiate upload
        InitiateUploadRequest initRequest = InitiateUploadRequest.newBuilder()
            .setDrive(testDrive.name)
            .setName("test-file.txt")
            .setMimeType("text/plain")
            .setExpectedSize(1000) // 1KB for test
            .build();
            
        LOG.infof("Initiating upload with name=%s, expectedSize=%d", 
            initRequest.getName(), initRequest.getExpectedSize());
            
        InitiateUploadResponse initResponse = nodeUploadService
            .initiateUpload(initRequest)
            .await().indefinitely();
            
        assertThat(initResponse, is(notNullValue()));
        assertThat(initResponse.getNodeId(), is(notNullValue()));
        assertThat(initResponse.getUploadId(), is(notNullValue()));
        assertThat(initResponse.getState(), is(UploadState.UPLOAD_STATE_PENDING));
        
        LOG.infof("Upload initiated: nodeId=%s, uploadId=%s", 
            initResponse.getNodeId(), initResponse.getUploadId());
        
        // Step 2: Create test data as chunks
        String testData = "Hello World! This is a test file content.";
        byte[] dataBytes = testData.getBytes();
        int chunkSize = 10; // Small chunks for testing
        
        List<UploadChunkRequest> chunks = new ArrayList<>();
        for (int i = 0; i < dataBytes.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, dataBytes.length);
            byte[] chunkData = new byte[end - i];
            System.arraycopy(dataBytes, i, chunkData, 0, end - i);
            
            UploadChunkRequest chunk = UploadChunkRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .setUploadId(initResponse.getUploadId())
                .setChunkNumber(chunks.size() + 1)
                .setData(ByteString.copyFrom(chunkData))
                .setIsLast(end >= dataBytes.length)
                .build();
                
            chunks.add(chunk);
        }
        
        LOG.infof("Created %d chunks for upload", chunks.size());
        
        // Step 3: Upload chunks
        List<UploadChunkResponse> uploadResponses = nodeUploadService
            .uploadChunks(Multi.createFrom().items(chunks.stream()))
            .collect().asList()
            .await().indefinitely();
            
        // Get the last response (final state)
        UploadChunkResponse uploadResponse = uploadResponses.get(uploadResponses.size() - 1);
            
        assertThat(uploadResponse, is(notNullValue()));
        assertThat(uploadResponse.getState(), is(UploadState.UPLOAD_STATE_COMPLETED));
        assertThat(uploadResponse.getBytesUploaded(), is((long)dataBytes.length));
        assertThat(uploadResponse.getChunkNumber(), is((long)chunks.size()));
        
        LOG.infof("Upload completed: %d bytes in %d chunks", 
            uploadResponse.getBytesUploaded(), uploadResponse.getChunkNumber());
    }
    
    @Test
    @TestTransaction
    void testImmediateSmallUpload() {
        LOG.info("Starting immediate small upload test");
        
        // Create test drive for upload
        Drive testDrive = entityHelper.createTestDrive("immediate-test", bucketManager.getCurrentBucket());
        
        String testData = "Small test file";
        byte[] dataBytes = testData.getBytes();
        
        // Test immediate upload with payload
        InitiateUploadRequest initRequest = InitiateUploadRequest.newBuilder()
            .setDrive(testDrive.name)
            .setName("small-file.txt")
            .setMimeType("text/plain")
            .setExpectedSize(dataBytes.length)
            .setPayload(Any.newBuilder()
                .setValue(ByteString.copyFrom(dataBytes))
                .setTypeUrl("type.googleapis.com/test.TestData")
                .build())
            .build();
            
        InitiateUploadResponse initResponse = nodeUploadService
            .initiateUpload(initRequest)
            .await().indefinitely();
            
        assertThat(initResponse, is(notNullValue()));
        assertThat(initResponse.getState(), is(UploadState.UPLOAD_STATE_COMPLETED));
        
        LOG.infof("Immediate upload completed: nodeId=%s", initResponse.getNodeId());
    }
}