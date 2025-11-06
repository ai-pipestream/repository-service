package io.pipeline.repository.service;

import io.pipeline.repository.entity.*;
import io.pipeline.repository.exception.*;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.DocumentServiceTestHelper;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.OffsetDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
public class DocumentServiceTest {
    
    private static final Logger LOG = Logger.getLogger(DocumentServiceTest.class);
    
    @Inject
    DocumentService documentService;
    
    @Inject
    TestBucketManager bucketManager;
    
    @Inject
    DocumentServiceTestHelper documentHelper;
    
    private String testBucket;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Set up S3 bucket only
        testBucket = bucketManager.setupBucket(testInfo);
        LOG.infof("S3 bucket setup complete: %s", testBucket);
    }
    
    @AfterEach
    void tearDown() {
        bucketManager.cleanupBucket(testBucket);
    }
    
    @Test
    @TestTransaction
    void testCreateDocumentSuccess() {
        LOG.info("Testing successful document creation");
        
        // Create test document using helper
        DocumentServiceTestHelper.TestDocument testDoc = documentHelper.createTestDocument(
            testBucket, "create-test-drive", "test-doc.txt");
        
        DocumentService.DocumentResult result = testDoc.result;
        
        // Verify result
        assertThat(result.success, is(true));
        assertThat(result.nodeEntity, is(notNullValue()));
        assertThat(result.s3Metadata, is(notNullValue()));
        
        // Verify database record
        Node dbNode = result.nodeEntity;
        assertThat(dbNode.id, is(notNullValue()));
        assertThat(dbNode.documentId, is(notNullValue()));
        assertThat(dbNode.name, is("test-doc.txt"));
        assertThat(dbNode.driveId, is(testDoc.drive.id));
        assertThat(dbNode.contentType, is("text/plain"));
        assertThat(dbNode.size, is((long) testDoc.originalPayload.length));
        assertThat(dbNode.s3Key, is(dbNode.documentId + ".pb"));
        
        // Verify S3 metadata
        assertThat(result.s3Metadata.s3Key, is(dbNode.s3Key));
        assertThat(result.s3Metadata.size, is((long) testDoc.originalPayload.length));
        
        LOG.infof("✅ Document created successfully: id=%d, documentId=%s", 
            dbNode.id, dbNode.documentId);
    }
    
    @Test
    @TestTransaction
    void testCreateDocumentWithInvalidDrive() {
        LOG.info("Testing document creation with invalid drive");
        
        byte[] payload = "test data".getBytes();
        
        DriveNotFoundException exception = assertThrows(DriveNotFoundException.class, () -> {
            documentService.createDocument(
                "non-existent-drive", 
                "test-doc.txt", 
                "text/plain", 
                payload, 
                "type.googleapis.com/test.TestData"
            );
        });
        
        assertThat(exception.getErrorCode(), is("DRIVE_NOT_FOUND"));
        assertThat(exception.getOperation(), is("findDrive"));
        assertThat(exception.getMessage(), containsString("non-existent-drive"));
        
        LOG.infof("✅ Correctly threw DriveNotFoundException: %s", exception.getStructuredMessage());
    }
    
    @Test
    @TestTransaction
    void testCreateDocumentWithEmptyPayload() {
        LOG.info("Testing document creation with empty payload");
        
        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> {
            documentService.createDocument(
                "any-drive-name", // Drive won't be looked up due to validation failure
                "test-doc.txt", 
                "text/plain", 
                new byte[0], // Empty payload
                "type.googleapis.com/test.TestData"
            );
        });
        
        assertThat(exception.getErrorCode(), is("INVALID_REQUEST"));
        assertThat(exception.getOperation(), is("createDocument"));
        assertThat(exception.getMessage(), containsString("payload cannot be empty"));
        
        LOG.infof("✅ Correctly threw InvalidRequestException: %s", exception.getStructuredMessage());
    }
    
    @Test
    @TestTransaction
    void testCreateDocumentWithMissingName() {
        LOG.info("Testing document creation with missing name");
        
        byte[] payload = "test data".getBytes();
        
        InvalidRequestException exception = assertThrows(InvalidRequestException.class, () -> {
            documentService.createDocument(
                "any-drive-name", // Drive won't be looked up due to validation failure
                "", // Empty name
                "text/plain", 
                payload, 
                "type.googleapis.com/test.TestData"
            );
        });
        
        assertThat(exception.getErrorCode(), is("INVALID_REQUEST"));
        assertThat(exception.getMessage(), containsString("name"));
        assertThat(exception.getMessage(), containsString("required"));
        
        LOG.infof("✅ Correctly threw InvalidRequestException for missing name: %s", 
            exception.getStructuredMessage());
    }
    
    @Test
    @TestTransaction
    void testGetDocumentSuccess() {
        LOG.info("Testing successful document retrieval");
        
        // Create test document using helper
        DocumentServiceTestHelper.TestDocument testDoc = documentHelper.createTestDocument(
            testBucket, "get-test-drive", "retrieve-test.txt");
        
        String documentId = testDoc.getDocumentId();
        
        // Test get without payload
        DocumentService.DocumentResult getResult = documentService.getDocument(documentId, false);
        
        assertThat(getResult.success, is(true));
        assertThat(getResult.nodeEntity, is(notNullValue()));
        assertThat(getResult.nodeEntity.documentId, is(documentId));
        assertThat(getResult.payload, is(nullValue())); // No payload requested
        
        // Test get with payload
        DocumentService.DocumentResult getWithPayload = documentService.getDocument(documentId, true);
        
        assertThat(getWithPayload.success, is(true));
        assertThat(getWithPayload.payload, is(notNullValue()));
        assertThat(getWithPayload.payload, is(testDoc.originalPayload));
        
        LOG.infof("✅ Document retrieval working: with and without payload");
    }
    
    @Test
    void testGetDocumentNotFound() {
        LOG.info("Testing document retrieval with non-existent ID");
        
        DocumentNotFoundException exception = assertThrows(DocumentNotFoundException.class, () -> {
            documentService.getDocument("non-existent-doc-id", false);
        });
        
        assertThat(exception.getErrorCode(), is("DOCUMENT_NOT_FOUND"));
        assertThat(exception.getOperation(), is("findDocument"));
        assertThat(exception.getMessage(), containsString("non-existent-doc-id"));
        
        LOG.infof("✅ Correctly threw DocumentNotFoundException: %s", exception.getStructuredMessage());
    }
}
