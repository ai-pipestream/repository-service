package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SearchIndexEventEmitter.
 * 
 * Tests the search index event emitter functionality and integration with KafkaKeyStrategy.
 * Focuses on document-centric partitioning using documentId (PipeDoc ID) to ensure
 * search index events are processed in the same partition as document events.
 */
@ExtendWith(MockitoExtension.class)
class SearchIndexEventEmitterTest {

    @Mock
    private MutinyEmitter<SearchIndexEvent> mockMutinyEmitter;
    
    @Mock
    private KafkaKeyStrategy mockKeyStrategy;
    
    private SearchIndexEventEmitter searchIndexEventEmitter;
    private SearchIndexEvent testSearchIndexEvent;

    @BeforeEach
    void setUp() {
        searchIndexEventEmitter = new SearchIndexEventEmitter(mockMutinyEmitter, mockKeyStrategy);
        
        testSearchIndexEvent = SearchIndexEvent.newBuilder()
            .setEventId("event-123")
            .setDocumentId("pipe-doc-123")
            .setDriveName("test-drive")
            .setIndexName("search-index")
            .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                .setDocumentType("PDF")
                .setContentType("application/pdf")
                .setSize(1024L)
                .setPath("/test/path/document.pdf")
                .build())
            .build();
    }

    @Test
    @DisplayName("Should successfully send SearchIndexEvent using send() method")
    void shouldSuccessfullySendSearchIndexEventUsingSend() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testSearchIndexEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending search index event
        Uni<Void> result = searchIndexEventEmitter.send(testSearchIndexEvent);

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testSearchIndexEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        assertThat(result)
            .as("Result should be a Uni<Void>")
            .isNotNull();
    }

    @Test
    @DisplayName("Should successfully send SearchIndexEvent using sendAndForget() method")
    void shouldSuccessfullySendSearchIndexEventUsingSendAndForget() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testSearchIndexEvent)).thenReturn(expectedKey);

        // When: Sending search index event with sendAndForget
        assertThatCode(() -> searchIndexEventEmitter.sendAndForget(testSearchIndexEvent))
            .as("sendAndForget should not throw any exceptions")
            .doesNotThrowAnyException();

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testSearchIndexEvent);
        verify(mockMutinyEmitter).sendMessageAndForget(any());
    }

    @Test
    @DisplayName("Should generate key using documentId (PipeDoc ID) for SearchIndexEvent")
    void shouldGenerateKeyUsingDocumentIdForSearchIndexEvent() {
        // Given: SearchIndexEvent with specific documentId
        SearchIndexEvent event = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-98765")
            .setDriveName("test-drive")
            .setIndexName("specific-index")
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes("pipe-doc-98765".getBytes());
        when(mockKeyStrategy.getKey(event)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending search index event
        searchIndexEventEmitter.send(event);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(event);
    }

    @Test
    @DisplayName("Should handle different SearchIndexEvent types")
    void shouldHandleDifferentSearchIndexEventTypes() {
        // Given: Different types of search index events
        SearchIndexEvent requestedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-111")
            .setDriveName("drive-111")
            .setIndexName("index-111")
            .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                .setDocumentType("PDF")
                .setContentType("application/pdf")
                .setSize(2048L)
                .setPath("/path/requested.pdf")
                .build())
            .build();
        
        SearchIndexEvent completedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-222")
            .setDriveName("drive-222")
            .setIndexName("index-222")
            .setIndexCompleted(SearchIndexEvent.SearchIndexCompleted.newBuilder()
                .setIndexDocumentId("index-doc-222")
                .setChunkCount(5)
                .setEmbeddingCount(10)
                .build())
            .build();
        
        SearchIndexEvent failedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-333")
            .setDriveName("drive-333")
            .setIndexName("index-333")
            .setIndexFailed(SearchIndexEvent.SearchIndexFailed.newBuilder()
                .setErrorMessage("Indexing failed due to invalid content")
                .setErrorCode("INVALID_CONTENT")
                .setRetryable(true)
                .build())
            .build();
        
        SearchIndexEvent deletedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-444")
            .setDriveName("drive-444")
            .setIndexName("index-444")
            .setIndexDeleted(SearchIndexEvent.SearchIndexDeleted.newBuilder()
                .setReason("Document was deleted")
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(any())).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending different search index events
        searchIndexEventEmitter.send(requestedEvent);
        searchIndexEventEmitter.send(completedEvent);
        searchIndexEventEmitter.send(failedEvent);
        searchIndexEventEmitter.send(deletedEvent);

        // Then: Should handle all event types
        verify(mockKeyStrategy, times(4)).getKey(any());
        verify(mockMutinyEmitter, times(4)).sendMessage(any());
    }

    @Test
    @DisplayName("Should ensure same documentId generates same key for consistent partitioning with DocumentEvent")
    void shouldEnsureSameDocumentIdGeneratesSameKeyForConsistentPartitioningWithDocumentEvent() {
        // Given: SearchIndexEvent and DocumentEvent with same documentId
        String documentId = "pipe-doc-consistent";
        
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
            .setDocumentId(documentId)
            .setDriveName("drive-1")
            .setIndexName("index-1")
            .build();
        
        // Mock the key strategy to return the same key for both events
        UUID expectedKey = UUID.nameUUIDFromBytes(documentId.getBytes());
        when(mockKeyStrategy.getKey(searchEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending search index event
        searchIndexEventEmitter.send(searchEvent);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(searchEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        // This test verifies that the key strategy is called correctly.
        // The actual key generation logic is tested in KafkaKeyStrategyTest.
    }

    @Test
    @DisplayName("Should handle search index requested events with metadata")
    void shouldHandleSearchIndexRequestedEventsWithMetadata() {
        // Given: Search index requested event with metadata
        SearchIndexEvent requestedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-metadata")
            .setDriveName("test-drive")
            .setIndexName("metadata-index")
            .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                .setDocumentType("TEXT")
                .setContentType("text/plain")
                .setSize(512L)
                .setPath("/test/path/metadata.txt")
                .setMetadata(com.google.protobuf.Any.pack(
                    com.google.protobuf.StringValue.of("test-metadata")))
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(requestedEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending requested event
        searchIndexEventEmitter.send(requestedEvent);

        // Then: Should handle requested event with metadata
        verify(mockKeyStrategy).getKey(requestedEvent);
        verify(mockMutinyEmitter).sendMessage(any());
    }

    @Test
    @DisplayName("Should handle search index failed events with retry information")
    void shouldHandleSearchIndexFailedEventsWithRetryInformation() {
        // Given: Search index failed event with retry information
        SearchIndexEvent failedEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-failed")
            .setDriveName("test-drive")
            .setIndexName("failed-index")
            .setIndexFailed(SearchIndexEvent.SearchIndexFailed.newBuilder()
                .setErrorMessage("Network timeout during indexing")
                .setErrorCode("NETWORK_TIMEOUT")
                .setRetryable(true)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(failedEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending failed event
        searchIndexEventEmitter.send(failedEvent);

        // Then: Should handle failed event
        verify(mockKeyStrategy).getKey(failedEvent);
        verify(mockMutinyEmitter).sendMessage(any());
    }

    @Test
    @DisplayName("Should propagate exceptions from MutinyEmitter")
    void shouldPropagateExceptionsFromMutinyEmitter() {
        // Given: MutinyEmitter throws exception
        RuntimeException expectedException = new RuntimeException("Kafka send failed");
        when(mockKeyStrategy.getKey(testSearchIndexEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any()))
            .thenReturn(Uni.createFrom().failure(expectedException));

        // When/Then: Should propagate the exception
        assertThatThrownBy(() -> searchIndexEventEmitter.send(testSearchIndexEvent).await().indefinitely())
            .as("Should propagate MutinyEmitter exceptions")
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Kafka send failed");
    }

    @Test
    @DisplayName("Should handle edge cases for SearchIndexEvent")
    void shouldHandleEdgeCasesForSearchIndexEvent() {
        // Test with empty documentId
        SearchIndexEvent emptyDocEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("")
            .setDriveName("test-drive")
            .setIndexName("empty-index")
            .build();
        
        when(mockKeyStrategy.getKey(emptyDocEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());
        
        assertThatCode(() -> searchIndexEventEmitter.send(emptyDocEvent))
            .as("Should handle empty documentId")
            .doesNotThrowAnyException();
        
        // Test with empty index name
        SearchIndexEvent emptyIndexEvent = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-empty-index")
            .setDriveName("test-drive")
            .setIndexName("")
            .build();
        
        when(mockKeyStrategy.getKey(emptyIndexEvent)).thenReturn(UUID.randomUUID());
        
        assertThatCode(() -> searchIndexEventEmitter.sendAndForget(emptyIndexEvent))
            .as("Should handle empty index name")
            .doesNotThrowAnyException();
        
        verify(mockKeyStrategy, times(2)).getKey(any());
    }

    @Test
    @DisplayName("Should maintain consistent behavior across multiple sends")
    void shouldMaintainConsistentBehaviorAcrossMultipleSends() {
        // Given: Same search index event sent multiple times
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testSearchIndexEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending the same event multiple times
        searchIndexEventEmitter.send(testSearchIndexEvent);
        searchIndexEventEmitter.send(testSearchIndexEvent);
        searchIndexEventEmitter.sendAndForget(testSearchIndexEvent);

        // Then: Should call key strategy and MutinyEmitter consistently
        verify(mockKeyStrategy, times(3)).getKey(testSearchIndexEvent);
        verify(mockMutinyEmitter, times(2)).sendMessage(any());
        verify(mockMutinyEmitter, times(1)).sendMessageAndForget(any());
    }
}