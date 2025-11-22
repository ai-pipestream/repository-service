package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.*;
import io.pipeline.repository.test.DocumentServiceTestHelper;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EntityTestHelper;
import io.pipeline.repository.entity.Drive;
import io.pipeline.repository.entity.Node;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class GetNodeServiceTest {
    
    private static final Logger LOG = Logger.getLogger(GetNodeServiceTest.class);
    
    @Inject
    @io.quarkus.grpc.GrpcService
    FilesystemServiceImpl filesystemService;
    
    @Inject
    TestBucketManager bucketManager;
    
    @Inject
    EntityTestHelper entityHelper;
    
    @Inject
    DocumentServiceTestHelper documentHelper;
    
    private String testBucket;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        testBucket = bucketManager.setupBucket(testInfo);
    }
    
    @AfterEach
    void tearDown(TestInfo testInfo) {
        bucketManager.cleanupBucket(testBucket);
        LOG.infof("Cleaned up test resources for: %s", testInfo.getTestMethod().map(m -> m.getName()).orElse("unknownMethod"));
    }
    
    @Test
    @TestTransaction
    void testGetNodeWithoutPayload() {
        LOG.info("Testing getNode without payload hydration");
        
        // Create document via DocumentService (this will create the drive)
        DocumentServiceTestHelper.TestDocument testDoc = 
            documentHelper.createTestDocument(
                testBucket,
                "getnode-test",
                "getnode-test-doc.txt"
            );
        
        LOG.infof("Created document: id=%d, documentId=%s", 
            testDoc.result.nodeEntity.id, testDoc.result.nodeEntity.documentId);
        
        // Test getNode without payload
        GetNodeRequest request = GetNodeRequest.newBuilder()
            .setDrive(testDoc.drive.name)
            .setDocumentId(testDoc.result.nodeEntity.documentId)
            .setIncludePayload(false)
            .build();
        
        LOG.infof("📤 Calling getNode without payload");
        
        io.pipeline.repository.filesystem.Node retrievedNode = filesystemService
            .getNode(request)
            .await().indefinitely();
        
        // Verify the response
        assertThat("Node should be retrieved", retrievedNode, is(notNullValue()));
        assertThat("Node should have correct document ID", retrievedNode.getDocumentId(), is(testDoc.result.nodeEntity.documentId));
        assertThat("Node should have correct name", retrievedNode.getName(), is("getnode-test-doc.txt"));
        assertThat("Node should have correct drive ID", retrievedNode.getDriveId(), is(testDoc.drive.id));
        assertThat("Node should have correct content type", retrievedNode.getContentType(), is("text/plain"));
        assertThat("Node should have correct size", retrievedNode.getSizeBytes(), is((long) testDoc.originalPayload.length));
        assertThat("Node should have S3 key", retrievedNode.getS3Key(), is(notNullValue()));
        assertThat("Node should NOT have payload when includePayload=false", 
            retrievedNode.getPayload().getTypeUrl(), is(""));
        
        LOG.infof("✅ getNode without payload working: documentId=%s, size=%d", 
            retrievedNode.getDocumentId(), retrievedNode.getSizeBytes());
    }
    
    @Test
    @TestTransaction
    void testGetNodeWithPayload() {
        LOG.info("Testing getNode with payload hydration");
        
        // Create document via DocumentService (this will create the drive)
        DocumentServiceTestHelper.TestDocument testDoc = 
            documentHelper.createTestDocument(
                testBucket,
                "getnode-payload-test",
                "getnode-payload-test-doc.txt"
            );
        
        LOG.infof("Created document: id=%d, documentId=%s", 
            testDoc.result.nodeEntity.id, testDoc.result.nodeEntity.documentId);
        
        // Test getNode with payload
        GetNodeRequest request = GetNodeRequest.newBuilder()
            .setDrive(testDoc.drive.name)
            .setDocumentId(testDoc.result.nodeEntity.documentId)
            .setIncludePayload(true)
            .build();
        
        LOG.infof("📤 Calling getNode with payload hydration");
        
        io.pipeline.repository.filesystem.Node retrievedNode = filesystemService
            .getNode(request)
            .await().indefinitely();
        
        // Verify the response
        assertThat("Node should be retrieved", retrievedNode, is(notNullValue()));
        assertThat("Node should have correct document ID", retrievedNode.getDocumentId(), is(testDoc.result.nodeEntity.documentId));
        assertThat("Node should have correct name", retrievedNode.getName(), is("getnode-payload-test-doc.txt"));
        assertThat("Node should have correct drive ID", retrievedNode.getDriveId(), is(testDoc.drive.id));
        assertThat("Node should have correct content type", retrievedNode.getContentType(), is("text/plain"));
        assertThat("Node should have correct size", retrievedNode.getSizeBytes(), is((long) testDoc.originalPayload.length));
        assertThat("Node should have S3 key", retrievedNode.getS3Key(), is(notNullValue()));
        assertThat("Node should have payload when includePayload=true", 
            retrievedNode.getPayload().getTypeUrl(), is(not("")));
        
        // Verify payload content
        StringValue hydratedPayload;
        try {
            hydratedPayload = retrievedNode.getPayload().unpack(StringValue.class);
            assertThat("Hydrated payload should match original", 
                hydratedPayload.getValue(), is(new String(testDoc.originalPayload)));
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to unpack payload", e);
        }
        
        LOG.infof("✅ getNode with payload working: documentId=%s, hydratedSize=%d", 
            retrievedNode.getDocumentId(), hydratedPayload.getValue().length());
    }
    
    @Test
    @TestTransaction
    void testGetNodeNotFound() {
        LOG.info("Testing getNode with non-existent document");
        
        // Create test drive
        Drive testDrive = entityHelper.createTestDrive("getnode-notfound-test", testBucket);
        
        // Try to get non-existent document
        GetNodeRequest request = GetNodeRequest.newBuilder()
            .setDrive(testDrive.name)
            .setDocumentId("non-existent-document-id")
            .setIncludePayload(false)
            .build();
        
        LOG.infof("📤 Calling getNode with non-existent document");
        
        try {
            filesystemService
                .getNode(request)
                .await().indefinitely();
            
            // Should not reach here
            assertThat("Should have thrown exception", false, is(true));
            
        } catch (Exception e) {
            // Verify it's the expected exception
            assertThat("Should be StatusRuntimeException", e, is(instanceOf(io.grpc.StatusRuntimeException.class)));
            io.grpc.StatusRuntimeException statusException = (io.grpc.StatusRuntimeException) e;
            assertThat("Should be INVALID_ARGUMENT status", 
                statusException.getStatus().getCode(), is(io.grpc.Status.Code.INVALID_ARGUMENT));
            
            LOG.infof("✅ getNode correctly failed for non-existent document: %s", e.getMessage());
        }
    }
}
