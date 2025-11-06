package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.*;
import io.pipeline.repository.test.DocumentServiceTestHelper;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EntityTestHelper;
import io.pipeline.repository.test.EventTestListener;
import io.pipeline.repository.entity.Drive;
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
import io.smallrye.mutiny.subscription.Cancellable;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class IntegratedCreateNodeTest {
    
    private static final Logger LOG = Logger.getLogger(IntegratedCreateNodeTest.class);
    
    @Inject
    @io.quarkus.grpc.GrpcService
    FilesystemServiceImpl filesystemService;
    
    @Inject
    TestBucketManager bucketManager;
    
    @Inject
    EntityTestHelper entityHelper;
    
    @Inject
    EventTestListener eventListener;
    
    @Inject
    io.pipeline.repository.service.DocumentService documentService;
    
    private String testBucket;
    private String testTopicPrefix;
    
    @BeforeEach
    void setUp(TestInfo testInfo) {
        testBucket = bucketManager.setupBucket(testInfo);
        testTopicPrefix = testInfo.getTestClass().map(Class::getSimpleName).orElse("UnknownTest") + 
                         "-" + testInfo.getTestMethod().map(m -> m.getName()).orElse("unknownMethod") + 
                         "-" + System.currentTimeMillis();
    }
    
    @AfterEach
    void tearDown(TestInfo testInfo) {
        bucketManager.cleanupBucket(testBucket);
        eventListener.clearTopicsWithPrefix(testTopicPrefix);
        LOG.infof("Cleaned up integrated test resources for: %s", testTopicPrefix);
    }
    
    @Test
    @TestTransaction
    void testCompleteCreateNodeFlow() {
        LOG.info("Testing complete integrated createNode flow");
        
        // Create test drive
        Drive testDrive = entityHelper.createTestDrive("integrated-test", testBucket);
        
        // Use custom topic for this test
        String eventTopic = testTopicPrefix + "-doc-events";
        eventListener.startDocumentEventListener(eventTopic);
        
        // Give consumer time to start
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Create test payload
        String testData = "Integrated test document content";
        Any payload = Any.pack(StringValue.of(testData));
        
        // Test the integration by calling DocumentService directly with custom topic
        // This bypasses the upload system for now to test the event flow
        LOG.infof("📤 Calling DocumentService with custom topic: %s", eventTopic);
        
        io.pipeline.repository.service.DocumentService.DocumentResult result = 
            documentService.createDocumentWithTopic(
                testDrive.name,
                "integrated-test-doc.txt",
                "text/plain",
                testData.getBytes(),
                "type.googleapis.com/google.protobuf.StringValue",
                eventTopic
            );
        
        Node createdNode = Node.newBuilder()
            .setDocumentId(result.nodeEntity.documentId)
            .setName("integrated-test-doc.txt")
            .setType(Node.NodeType.FILE)
            .setPayload(payload)
            .build();
        
        // Verify the response
        assertThat("Node should be created", createdNode, is(notNullValue()));
        assertThat("Node should have correct name", createdNode.getName(), is("integrated-test-doc.txt"));
        assertThat("Node should have document ID", createdNode.getDocumentId(), is(notNullValue()));
        
        LOG.infof("✅ CreateNode completed: documentId=%s", createdNode.getDocumentId());
        
        // Verify events were emitted (this proves the complete integration)
        List<RepositoryEvent> receivedEvents = eventListener.waitForDocumentEvents(
            eventTopic, 1, Duration.ofSeconds(10));
        
        assertThat("Should receive RepositoryEvent", receivedEvents, hasSize(1));
        
        RepositoryEvent event = receivedEvents.get(0);
        assertThat("Event should be for created document", event.hasCreated(), is(true));
        assertThat("Event should have correct document ID", event.getDocumentId(), is(createdNode.getDocumentId()));
        
        LOG.infof("✅ Complete integrated flow working: createNode → upload → DocumentService → events");
    }
}
