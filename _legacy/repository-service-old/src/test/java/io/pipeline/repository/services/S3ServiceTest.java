package io.pipeline.repository.services;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test S3Service in isolation.
 * Uses MinIO dev services for testing.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3ServiceTest {
    
    @Inject
    S3Service s3Service;
    
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_DOCUMENT_ID = "test-doc-123";
    
    @BeforeEach
    void setUp() {
        // Clear client cache before each test
        s3Service.clearClientCache();
        
        // Create test bucket
        s3Service.createBucketIfNotExists(TEST_BUCKET, "us-east-1");
    }
    
    @Test
    void testStoreAndRetrieveProtobuf() {
        // Test data
        byte[] testData = "Hello, S3!".getBytes();
        String contentType = "application/x-protobuf";
        
        // Store protobuf
        S3Service.S3ObjectMetadata metadata = s3Service.storeProtobuf(
            TEST_BUCKET, TEST_DOCUMENT_ID, testData, contentType);
        
        // Verify metadata
        assertThat(metadata).isNotNull();
        assertThat(metadata.s3Key).isEqualTo(TEST_DOCUMENT_ID + ".pb");
        assertThat(metadata.size).isEqualTo((long) testData.length);
        assertThat(metadata.contentType).isEqualTo(contentType);
        assertThat(metadata.eTag).isNotNull();
        
        // Retrieve protobuf
        byte[] retrievedData = s3Service.retrieveProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID);
        
        // Verify data
        assertThat(retrievedData).isEqualTo(testData);
    }
    
    @Test
    void testProtobufExists() {
        // Initially should not exist
        assertThat(s3Service.protobufExists(TEST_BUCKET, TEST_DOCUMENT_ID)).isFalse();
        
        // Store protobuf
        byte[] testData = "Test data".getBytes();
        s3Service.storeProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID, testData, "application/x-protobuf");
        
        // Should now exist
        assertThat(s3Service.protobufExists(TEST_BUCKET, TEST_DOCUMENT_ID)).isTrue();
    }
    
    @Test
    void testGetProtobufMetadata() {
        // Initially should return null
        S3Service.S3ObjectMetadata metadata = s3Service.getProtobufMetadata(TEST_BUCKET, TEST_DOCUMENT_ID);
        assertThat(metadata).isNull();
        
        // Store protobuf
        byte[] testData = "Test data".getBytes();
        String contentType = "application/x-protobuf";
        s3Service.storeProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID, testData, contentType);
        
        // Get metadata
        metadata = s3Service.getProtobufMetadata(TEST_BUCKET, TEST_DOCUMENT_ID);
        
        // Verify metadata
        assertThat(metadata).isNotNull();
        assertThat(metadata.s3Key).isEqualTo(TEST_DOCUMENT_ID + ".pb");
        assertThat(metadata.size).isEqualTo((long) testData.length);
        assertThat(metadata.contentType).isEqualTo(contentType);
        assertThat(metadata.eTag).isNotNull();
        assertThat(metadata.lastModified).isNotNull();
    }
    
    @Test
    void testDeleteProtobuf() {
        // Store protobuf
        byte[] testData = "Test data".getBytes();
        s3Service.storeProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID, testData, "application/x-protobuf");
        
        // Verify it exists
        assertThat(s3Service.protobufExists(TEST_BUCKET, TEST_DOCUMENT_ID)).isTrue();
        
        // Delete protobuf
        s3Service.deleteProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID);
        
        // Verify it's gone
        assertThat(s3Service.protobufExists(TEST_BUCKET, TEST_DOCUMENT_ID)).isFalse();
        
        // Should throw exception when trying to retrieve deleted file
        assertThatThrownBy(() -> s3Service.retrieveProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Protobuf not found");
    }
    
    @Test
    void testRetrieveNonExistentProtobuf() {
        assertThatThrownBy(() -> s3Service.retrieveProtobuf(TEST_BUCKET, "non-existent-doc"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Protobuf not found");
    }
    
    @Test
    void testMultipleDocuments() {
        // Store multiple documents
        String doc1 = "doc-1";
        String doc2 = "doc-2";
        byte[] data1 = "Data 1".getBytes();
        byte[] data2 = "Data 2".getBytes();
        
        s3Service.storeProtobuf(TEST_BUCKET, doc1, data1, "application/x-protobuf");
        s3Service.storeProtobuf(TEST_BUCKET, doc2, data2, "application/x-protobuf");
        
        // Verify both exist
        assertThat(s3Service.protobufExists(TEST_BUCKET, doc1)).isTrue();
        assertThat(s3Service.protobufExists(TEST_BUCKET, doc2)).isTrue();
        
        // Retrieve both
        byte[] retrieved1 = s3Service.retrieveProtobuf(TEST_BUCKET, doc1);
        byte[] retrieved2 = s3Service.retrieveProtobuf(TEST_BUCKET, doc2);
        
        assertThat(retrieved1).isEqualTo(data1);
        assertThat(retrieved2).isEqualTo(data2);
        
        // Delete one
        s3Service.deleteProtobuf(TEST_BUCKET, doc1);
        
        // Verify only one exists
        assertThat(s3Service.protobufExists(TEST_BUCKET, doc1)).isFalse();
        assertThat(s3Service.protobufExists(TEST_BUCKET, doc2)).isTrue();
    }
    
    @Test
    void testEmptyData() {
        // Store empty data
        byte[] emptyData = new byte[0];
        s3Service.storeProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID, emptyData, "application/x-protobuf");
        
        // Verify it exists
        assertThat(s3Service.protobufExists(TEST_BUCKET, TEST_DOCUMENT_ID)).isTrue();
        
        // Retrieve it
        byte[] retrievedData = s3Service.retrieveProtobuf(TEST_BUCKET, TEST_DOCUMENT_ID);
        assertThat(retrievedData).isEmpty();
        
        // Verify metadata
        S3Service.S3ObjectMetadata metadata = s3Service.getProtobufMetadata(TEST_BUCKET, TEST_DOCUMENT_ID);
        assertThat(metadata.size).isEqualTo(0L);
    }
}