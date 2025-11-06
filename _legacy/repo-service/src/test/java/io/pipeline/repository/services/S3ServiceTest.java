package io.pipeline.repository.services;

import io.pipeline.repository.test.TestBucketManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class S3ServiceTest {
    
    @Inject
    S3Service s3Service;
    
    @Inject
    TestBucketManager bucketManager;
    
    private String testBucket;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        testBucket = bucketManager.setupBucket(testInfo);
    }
    
    @AfterEach
    void tearDown() {
        bucketManager.cleanupBucket(testBucket);
    }
    
    @Test
    void testStoreAndRetrieveProtobuf() {
        // Given
        String documentId = "test-document-123";
        byte[] testData = "Hello, S3!".getBytes();
        String contentType = "application/octet-stream";
        
        // When - store
        S3Service.S3ObjectMetadata metadata = s3Service.storeProtobuf(testBucket, documentId, testData, contentType);
        
        // Then - verify metadata
        assertNotNull(metadata);
        assertEquals(documentId + ".pb", metadata.s3Key);
        assertEquals((long) testData.length, metadata.size);
        assertEquals(contentType, metadata.contentType);
        assertNotNull(metadata.eTag);
        
        // When - retrieve
        byte[] retrievedData = s3Service.retrieveProtobuf(testBucket, documentId);
        
        // Then - verify data
        assertArrayEquals(testData, retrievedData);
    }
    
    @Test
    void testProtobufExists() {
        // Given
        String documentId = "test-exists-123";
        byte[] testData = "Test data".getBytes();
        
        // When - check before storing
        boolean existsBefore = s3Service.protobufExists(testBucket, documentId);
        
        // Then - should not exist
        assertFalse(existsBefore);
        
        // When - store and check again
        s3Service.storeProtobuf(testBucket, documentId, testData, "text/plain");
        boolean existsAfter = s3Service.protobufExists(testBucket, documentId);
        
        // Then - should exist
        assertTrue(existsAfter);
    }
    
    @Test
    void testGetProtobufMetadata() {
        // Given
        String documentId = "test-metadata-123";
        byte[] testData = "Metadata test data".getBytes();
        String contentType = "application/json";
        
        // When - store and get metadata
        s3Service.storeProtobuf(testBucket, documentId, testData, contentType);
        S3Service.S3ObjectMetadata metadata = s3Service.getProtobufMetadata(testBucket, documentId);
        
        // Then - verify metadata
        assertNotNull(metadata);
        assertEquals(documentId + ".pb", metadata.s3Key);
        assertEquals((long) testData.length, metadata.size);
        assertEquals(contentType, metadata.contentType);
        assertNotNull(metadata.eTag);
        assertNotNull(metadata.lastModified);
    }
    
    @Test
    void testDeleteProtobuf() {
        // Given
        String documentId = "test-delete-123";
        byte[] testData = "Delete test data".getBytes();
        
        // When - store, verify exists, then delete
        s3Service.storeProtobuf(testBucket, documentId, testData, "text/plain");
        assertTrue(s3Service.protobufExists(testBucket, documentId));
        
        s3Service.deleteProtobuf(testBucket, documentId);
        
        // Then - should not exist after deletion
        assertFalse(s3Service.protobufExists(testBucket, documentId));
    }
    
    @Test
    void testRetrieveNonExistentProtobuf() {
        // Given
        String nonExistentId = "non-existent-123";
        
        // When/Then - should throw exception
        assertThrows(RuntimeException.class, () -> {
            s3Service.retrieveProtobuf(testBucket, nonExistentId);
        });
    }
}
