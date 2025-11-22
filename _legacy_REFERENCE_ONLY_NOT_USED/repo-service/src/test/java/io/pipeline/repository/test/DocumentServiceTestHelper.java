package io.pipeline.repository.test;

import io.pipeline.repository.entity.*;
import io.pipeline.repository.service.DocumentService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Test helper for DocumentService tests.
 * Provides common patterns for document service testing.
 */
@ApplicationScoped
public class DocumentServiceTestHelper {
    
    private static final Logger LOG = Logger.getLogger(DocumentServiceTestHelper.class);
    
    @Inject
    EntityTestHelper entityHelper;
    
    @Inject
    DocumentService documentService;
    
    /**
     * Create a test document with drive setup.
     */
    public TestDocument createTestDocument(String testBucket, String driveName, String documentName) {
        // Create drive
        Drive drive = entityHelper.createTestDrive(driveName, testBucket);
        
        // Create document
        String testData = "Test document content for " + documentName;
        byte[] payload = testData.getBytes();
        
        DocumentService.DocumentResult result = documentService.createDocument(
            drive.name, 
            documentName, 
            "text/plain", 
            payload, 
            "type.googleapis.com/test.TestData"
        );
        
        LOG.infof("Created test document: drive=%s, document=%s, id=%d", 
            drive.name, documentName, result.nodeEntity.id);
        
        return new TestDocument(drive, result, payload);
    }
    
    /**
     * Container for test document creation results.
     */
    public static class TestDocument {
        public final Drive drive;
        public final DocumentService.DocumentResult result;
        public final byte[] originalPayload;
        
        public TestDocument(Drive drive, DocumentService.DocumentResult result, byte[] originalPayload) {
            this.drive = drive;
            this.result = result;
            this.originalPayload = originalPayload;
        }
        
        public Node getNode() {
            return result.nodeEntity;
        }
        
        public String getDocumentId() {
            return result.nodeEntity.documentId;
        }
    }
}
