package io.pipeline.repository.services;

import io.pipeline.repository.test.TestBucketManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class ReactiveUploadServiceTest {
    
    private static final Logger LOG = Logger.getLogger(ReactiveUploadServiceTest.class);
    
    @Inject
    ReactiveUploadService reactiveUploadService;
    
    @Inject
    S3Service blockingS3Service; // For comparison
    
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
    void testReactiveStoreAndRetrieve() {
        LOG.info("Testing reactive S3 operations");
        
        String documentId = "reactive-test-doc";
        byte[] testData = "Hello, Reactive S3!".getBytes();
        String contentType = "text/plain";
        
        // Store reactively
        S3Service.S3ObjectMetadata metadata = reactiveUploadService
            .storeProtobufAsync(testBucket, documentId, testData, contentType)
            .await().indefinitely();
        
        // Verify metadata
        assertThat(metadata, is(notNullValue()));
        assertThat(metadata.s3Key, is(documentId + ".pb"));
        assertThat(metadata.size, is((long) testData.length));
        assertThat(metadata.contentType, is(contentType));
        assertThat(metadata.eTag, is(notNullValue()));
        
        LOG.infof("Stored reactively: key=%s, size=%d", metadata.s3Key, metadata.size);
        
        // Retrieve reactively
        byte[] retrievedData = reactiveUploadService
            .retrieveProtobufAsync(testBucket, documentId)
            .await().indefinitely();
        
        // Verify data
        assertThat(retrievedData, is(testData));
        
        LOG.infof("✅ Reactive S3 operations working correctly");
    }
    
    @Test
    void testReactiveVsBlockingPerformance() {
        LOG.info("Comparing reactive vs blocking S3 performance");
        
        String documentId = "perf-test-doc";
        byte[] testData = "Performance test data".getBytes();
        String contentType = "application/octet-stream";
        
        // Test blocking approach
        long blockingStart = System.currentTimeMillis();
        S3Service.S3ObjectMetadata blockingResult = blockingS3Service.storeProtobuf(
            testBucket, documentId + "-blocking", testData, contentType);
        long blockingTime = System.currentTimeMillis() - blockingStart;
        
        // Test reactive approach  
        long reactiveStart = System.currentTimeMillis();
        S3Service.S3ObjectMetadata reactiveResult = reactiveUploadService
            .storeProtobufAsync(testBucket, documentId + "-reactive", testData, contentType)
            .await().indefinitely();
        long reactiveTime = System.currentTimeMillis() - reactiveStart;
        
        LOG.infof("Performance comparison: blocking=%dms, reactive=%dms", blockingTime, reactiveTime);
        
        // Both should work
        assertThat(blockingResult.eTag, is(notNullValue()));
        assertThat(reactiveResult.eTag, is(notNullValue()));
        
        LOG.infof("✅ Both approaches working, reactive time: %dms, blocking time: %dms", 
            reactiveTime, blockingTime);
    }
    
    @Test
    void testReactiveExistsAndDelete() {
        LOG.info("Testing reactive exists and delete operations");
        
        String documentId = "exists-delete-test";
        byte[] testData = "Test data for exists/delete".getBytes();
        
        // Store first
        reactiveUploadService.storeProtobufAsync(testBucket, documentId, testData, "text/plain")
            .await().indefinitely();
        
        // Test exists
        Boolean exists = reactiveUploadService.protobufExistsAsync(testBucket, documentId)
            .await().indefinitely();
        assertThat("Should exist after store", exists, is(true));
        
        // Test delete
        reactiveUploadService.deleteProtobufAsync(testBucket, documentId)
            .await().indefinitely();
        
        // Test exists after delete
        Boolean existsAfterDelete = reactiveUploadService.protobufExistsAsync(testBucket, documentId)
            .await().indefinitely();
        assertThat("Should not exist after delete", existsAfterDelete, is(false));
        
        LOG.infof("✅ Reactive exists and delete operations working");
    }
}
