package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import io.pipeline.repository.kafka.DriveEventEmitter;
import io.pipeline.repository.kafka.DocumentEventEmitter;
import io.pipeline.repository.kafka.SearchIndexEventEmitter;
import io.pipeline.repository.kafka.RequestCountEventEmitter;
import io.pipeline.repository.kafka.KafkaKeyStrategy;
import io.pipeline.repository.services.EventPublisher;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test class for Apicurio Kafka integration testing.
 * 
 * This class provides common test functionality for both unit tests and integration tests.
 * Unit tests extend this class directly, while integration tests extend ApicurioKafkaIntegrationTestIT.
 * 
 * Tests validate:
 * - Correct MutinyEmitter + Apicurio Registry v3 integration
 * - Proper UUID key generation and partitioning
 * - Event publishing through RepoEmitter abstraction
 * - Message consumption and validation
 */
public abstract class BaseApicurioKafkaTest {

    @Inject
    DriveEventEmitter driveEventEmitter;
    
    @Inject
    DocumentEventEmitter documentEventEmitter;
    
    @Inject
    SearchIndexEventEmitter searchIndexEventEmitter;
    
    @Inject
    RequestCountEventEmitter requestCountEventEmitter;
    
    @Inject
    KafkaKeyStrategy keyStrategy;
    
    @Inject
    EventPublisher eventPublisher;

    @BeforeEach
    void setUp() {
        // Unit tests don't need to clear Kafka consumers
    }

    @Test
    void shouldInjectAllEmitters() {
        assertThat(driveEventEmitter).isNotNull();
        assertThat(documentEventEmitter).isNotNull();
        assertThat(searchIndexEventEmitter).isNotNull();
        assertThat(requestCountEventEmitter).isNotNull();
        assertThat(keyStrategy).isNotNull();
        assertThat(eventPublisher).isNotNull();
    }

    @Test
    void shouldGenerateConsistentKeysForDocumentEvents() {
        // Given
        String documentId = "doc-12345";
        DocumentEvent event = DocumentEvent.newBuilder()
                .setDocumentId(documentId)
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("test-doc")
                        .setDocumentType("pdf")
                        .setContentType("application/pdf")
                        .setSize(1024L)
                        .setS3Key("test-key")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateConsistentKeysForDriveEvents() {
        // Given
        DriveEvent event = DriveEvent.newBuilder()
                .setDriveId(12345L)
                .setCreated(DriveEvent.DriveCreated.newBuilder()
                        .setBucketName("test-bucket")
                        .setRegion("us-east-1")
                        .setDescription("Test drive")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateConsistentKeysForSearchIndexEvents() {
        // Given
        String documentId = "doc-67890";
        SearchIndexEvent event = SearchIndexEvent.newBuilder()
                .setDocumentId(documentId)
                .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                        .setDocumentType("pdf")
                        .setContentType("application/pdf")
                        .setSize(2048L)
                        .setPath("/test/path")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateConsistentKeysForRequestCountEvents() {
        // Given - Document request
        RequestCountEvent.DocumentRequestCount docRequest = RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("doc-request-123")
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(100L)
                .build();
        
        RequestCountEvent event = RequestCountEvent.newBuilder()
                .setDriveId(999L)
                .setDocumentRequest(docRequest)
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateDifferentKeysForDifferentDocuments() {
        // Given
        DocumentEvent event1 = DocumentEvent.newBuilder()
                .setDocumentId("doc-111")
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("doc1")
                        .setDocumentType("pdf")
                        .build())
                .build();
                
        DocumentEvent event2 = DocumentEvent.newBuilder()
                .setDocumentId("doc-222")
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("doc2")
                        .setDocumentType("pdf")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event1);
        UUID key2 = keyStrategy.getKey(event2);

        // Then
        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void shouldGenerateDifferentKeysForDifferentDrives() {
        // Given
        DriveEvent event1 = DriveEvent.newBuilder()
                .setDriveId(111L)
                .setCreated(DriveEvent.DriveCreated.newBuilder()
                        .setBucketName("bucket1")
                        .setRegion("us-east-1")
                        .build())
                .build();
                
        DriveEvent event2 = DriveEvent.newBuilder()
                .setDriveId(222L)
                .setCreated(DriveEvent.DriveCreated.newBuilder()
                        .setBucketName("bucket2")
                        .setRegion("us-west-2")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event1);
        UUID key2 = keyStrategy.getKey(event2);

        // Then
        assertThat(key1).isNotEqualTo(key2);
    }

    /**
     * Abstract method for testing actual message publishing.
     * Implementation depends on whether we're running unit tests or integration tests.
     */
    protected abstract void testMessagePublishing();

    /**
     * Helper method to wait for messages to be consumed.
     * Override in integration tests to wait for actual Kafka consumption.
     */
    protected void waitForMessages(int expectedCount) {
        // Default implementation for unit tests - no waiting needed
        // Integration tests will override this to wait for actual Kafka consumption
    }
}