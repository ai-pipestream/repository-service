package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for KafkaKeyStrategy.
 * 
 * This is a pure JUnit test that doesn't require Quarkus or any infrastructure.
 * It tests the key generation logic in isolation.
 */
public class KafkaKeyStrategyUnitTest {

    private KafkaKeyStrategy keyStrategy;

    @BeforeEach
    void setUp() {
        keyStrategy = new KafkaKeyStrategy();
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

    @Test
    void shouldUseDocumentIdForDocumentRelatedEvents() {
        // Given - Same document ID should produce same key regardless of event type
        String documentId = "doc-same-key-test";
        
        DocumentEvent docEvent = DocumentEvent.newBuilder()
                .setDocumentId(documentId)
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("test-doc")
                        .build())
                .build();
                
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
                .setDocumentId(documentId)
                .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                        .setDocumentType("pdf")
                        .build())
                .build();

        RequestCountEvent.DocumentRequestCount docRequest = RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(documentId)
                .setOperation("READ")
                .build();
        
        RequestCountEvent requestEvent = RequestCountEvent.newBuilder()
                .setDriveId(999L)
                .setDocumentRequest(docRequest)
                .build();

        // When
        UUID docKey = keyStrategy.getKey(docEvent);
        UUID searchKey = keyStrategy.getKey(searchEvent);
        UUID requestKey = keyStrategy.getKey(requestEvent);

        // Then - All events for the same document should have the same key
        assertThat(docKey).isEqualTo(searchKey);
        assertThat(docKey).isEqualTo(requestKey);
        assertThat(searchKey).isEqualTo(requestKey);
    }

    @Test
    void shouldThrowExceptionForUnknownMessageType() {
        // Given
        Object unknownMessage = "not a protobuf message";

        // When & Then
        assertThatThrownBy(() -> keyStrategy.getKey(unknownMessage))
                .isInstanceOf(IllegalArgumentException.class);
    }
}