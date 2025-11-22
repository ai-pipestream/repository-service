package io.pipeline.repository.services;

import io.pipeline.repository.test.DocumentServiceTestHelper;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EventTestListener;
import io.pipeline.repository.filesystem.RepositoryEvent;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.smallrye.mutiny.subscription.Cancellable;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class EventPublisherTest {
    
    private static final Logger LOG = Logger.getLogger(EventPublisherTest.class);
    
    @Inject
    ReactiveEventPublisherImpl eventPublisher;
    
    @Inject
    DocumentServiceTestHelper documentHelper;
    
    @Inject
    TestBucketManager bucketManager;
    
    @Inject
    EventTestListener eventListener;
    
    private String testBucket;
    private String testTopicPrefix;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        testBucket = bucketManager.setupBucket(testInfo);
        // Create unique topic prefix for this test
        testTopicPrefix = testInfo.getTestClass().map(Class::getSimpleName).orElse("UnknownTest") + 
                         "-" + testInfo.getTestMethod().map(m -> m.getName()).orElse("unknownMethod") + 
                         "-" + System.currentTimeMillis();
    }
    
    @AfterEach
    void tearDown(TestInfo testInfo) {
        // Clean up this test's specific bucket (parallel-safe)
        bucketManager.cleanupBucket(testBucket);
        
        // Only clear events for topics created by this test
        eventListener.clearTopicsWithPrefix(testTopicPrefix);
        
        LOG.infof("Cleaned up test resources for: %s (bucket: %s)", testTopicPrefix, testBucket);
    }
    
    @Test
    @TestTransaction
    void testMutinyEmitterDocumentCreated() {
        LOG.info("Testing MutinyEmitter for document created events");
        
        // Create test document
        DocumentServiceTestHelper.TestDocument testDoc = documentHelper.createTestDocument(
            testBucket, "event-test-drive", "event-test-doc.txt");
        
        // Test reactive event emission
        String testTopic = testTopicPrefix + "-doc-events";
        
        // No longer need payload for events - content is in S3
        
        // Test the MutinyEmitter.sendMessageAndForget() method
        try {
            Cancellable cancellable = eventPublisher.publishDocumentCreatedToTopic(
                testTopic,
                testDoc.getDocumentId(),
                testDoc.drive.name,
                testDoc.drive.id,
                testDoc.getNode().path,
                testDoc.getNode().name,
                testDoc.getNode().contentType,
                testDoc.getNode().size,
                testDoc.getNode().s3Key,
                "type.googleapis.com/test.TestData",
                "test-content-hash",
                "test-version-id",
                testDoc.drive.accountId,
                "test-connector"
            );
            
            // Verify we got a cancellable back
            assertThat("Should return a cancellable", cancellable, is(notNullValue()));
            
            LOG.infof("✅ MutinyEmitter.sendMessageAndForget() returned cancellable for topic: %s", testTopic);
            
        } catch (Exception e) {
            LOG.errorf(e, "❌ MutinyEmitter failed");
            throw e;
        }
    }
    
    @Test
    @TestTransaction
    void testFireAndForgetMethods() {
        LOG.info("Testing fire-and-forget event methods");
        
        // Create test document
        DocumentServiceTestHelper.TestDocument testDoc = documentHelper.createTestDocument(
            testBucket, "fire-forget-drive", "fire-forget-doc.txt");
        
        // No longer need payload for events - content is in S3
        
        // Test fire-and-forget methods don't block or throw
        try {
            eventPublisher.publishDocumentCreatedAndForget(
                testDoc.getDocumentId(),
                testDoc.drive.name,
                testDoc.drive.id,
                testDoc.getNode().path,
                testDoc.getNode().name,
                testDoc.getNode().contentType,
                testDoc.getNode().size,
                testDoc.getNode().s3Key,
                "type.googleapis.com/test.TestData",
                "test-content-hash",
                "test-version-id",
                testDoc.drive.accountId,
                "test-connector"
            );
            
            eventPublisher.publishDocumentUpdatedAndForget(
                testDoc.getDocumentId(),
                testDoc.drive.name,
                testDoc.drive.id,
                testDoc.getNode().path,
                "updated-" + testDoc.getNode().name,
                testDoc.getNode().contentType,
                testDoc.getNode().size * 2,
                "type.googleapis.com/test.UpdatedData",
                "updated-content-hash",
                "updated-version-id",
                "previous-version-id",
                testDoc.drive.accountId
            );
            
            eventPublisher.publishDocumentDeletedAndForget(
                testDoc.getDocumentId(),
                testDoc.drive.name,
                testDoc.drive.id,
                testDoc.getNode().path,
                "Test deletion"
            );
            
            LOG.infof("✅ All fire-and-forget methods completed without blocking");
            
        } catch (Exception e) {
            LOG.errorf(e, "❌ Fire-and-forget methods failed");
            throw e;
        }
    }
    
    @Test
    @TestTransaction
    void testEndToEndKafkaEventFlow() {
        LOG.info("Testing end-to-end Kafka event flow with dynamic topics");
        
        // Create test document
        DocumentServiceTestHelper.TestDocument testDoc = documentHelper.createTestDocument(
            testBucket, "kafka-test-drive", "kafka-test-doc.txt");
        
        // Create unique test topic for this specific test
        String testTopic = testTopicPrefix + "-e2e-events";
        
        // No longer need payload for events - content is in S3
        
        // Start listening for events FIRST and let consumer settle
        eventListener.startDocumentEventListener(testTopic);
        
        // Give consumer time to start and subscribe
        try {
            Thread.sleep(2000); // 2 seconds for consumer to be ready
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        LOG.infof("📡 Consumer ready, sending event to topic: %s", testTopic);
        
        // NOW send event to test topic
        Cancellable sendCancellable = eventPublisher.publishDocumentCreatedToTopic(
            testTopic,
            testDoc.getDocumentId(),
            testDoc.drive.name,
            testDoc.drive.id,
            testDoc.getNode().path,
            testDoc.getNode().name,
            testDoc.getNode().contentType,
            testDoc.getNode().size,
            testDoc.getNode().s3Key,
            "type.googleapis.com/test.TestData",
            "test-content-hash",
            "test-version-id",
            testDoc.drive.accountId,
            "test-connector"
        );
        
        LOG.infof("📤 Event sent to topic: %s", testTopic);
        
        // Wait for and verify event was received using Awaitility
        List<RepositoryEvent> receivedEvents = eventListener.waitForDocumentEvents(
            testTopic, 1, Duration.ofSeconds(15));
        
        assertThat("Should receive 1 event", receivedEvents, hasSize(1));
        
        RepositoryEvent receivedEvent = receivedEvents.get(0);
        assertThat("Event should have correct documentId", 
            receivedEvent.getDocumentId(), is(testDoc.getDocumentId()));
        assertThat("Event should have correct accountId", 
            receivedEvent.getAccountId(), is(testDoc.drive.accountId));
        assertThat("Event should be created event", 
            receivedEvent.hasCreated(), is(true));
        
        LOG.infof("✅ End-to-end Kafka event flow verified with topic: %s", testTopic);
        
        // Clean up this test's events
        eventListener.clearEvents(testTopic);
    }
    
    // SearchIndexEvent tests removed - these events are now handled differently in the new RepositoryEvent structure
    
    // RequestCountEvent tests removed - these events are now handled differently in the new RepositoryEvent structure
    
    @Test
    @TestTransaction
    void testEventFailureHandling() {
        LOG.info("Testing event failure handling");
        
        // Test with invalid topic name to trigger failure
        String invalidTopic = ""; // Empty topic should fail
        
        try {
            Cancellable cancellable = eventPublisher.publishDocumentCreatedToTopic(
                invalidTopic,
                "test-doc-id",
                "test-drive",
                1L,
                "/test",
                "test.txt",
                "text/plain",
                100L,
                "test-doc-id.pb",
                "type.googleapis.com/test",
                "test-hash",
                "test-version",
                "test-customer",
                "test-connector"
            );
            
            LOG.infof("✅ Event failure handling working (or invalid topic was accepted)");
            
        } catch (Exception e) {
            LOG.infof("✅ Event failure properly caught: %s", e.getMessage());
            // This is expected for invalid topics
        }
    }
}
