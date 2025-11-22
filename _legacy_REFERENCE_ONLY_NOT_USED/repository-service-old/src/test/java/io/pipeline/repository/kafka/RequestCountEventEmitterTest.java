package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.RequestCountEvent;
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
 * Unit tests for RequestCountEventEmitter.
 * 
 * Tests the request count event emitter functionality and integration with KafkaKeyStrategy.
 * Focuses on the dual key strategy: documentId when available (for document-related requests),
 * fallback to driveId for drive-related requests.
 */
@ExtendWith(MockitoExtension.class)
class RequestCountEventEmitterTest {

    @Mock
    private MutinyEmitter<RequestCountEvent> mockMutinyEmitter;
    
    @Mock
    private KafkaKeyStrategy mockKeyStrategy;
    
    private RequestCountEventEmitter requestCountEventEmitter;
    private RequestCountEvent testDocumentRequestEvent;
    private RequestCountEvent testDriveRequestEvent;

    @BeforeEach
    void setUp() {
        requestCountEventEmitter = new RequestCountEventEmitter(mockMutinyEmitter, mockKeyStrategy);
        
        testDocumentRequestEvent = RequestCountEvent.newBuilder()
            .setEventId("event-123")
            .setDriveName("test-drive")
            .setDriveId(12345L)
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("pipe-doc-123")
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(150L)
                .build())
            .build();
        
        testDriveRequestEvent = RequestCountEvent.newBuilder()
            .setEventId("event-456")
            .setDriveName("test-drive")
            .setDriveId(12345L)
            .setCustomerId("customer-123")
            .setDriveRequest(RequestCountEvent.DriveRequestCount.newBuilder()
                .setOperation("CREATE")
                .setProcessingTimeMs(200L)
                .build())
            .build();
    }

    @Test
    @DisplayName("Should successfully send RequestCountEvent using send() method")
    void shouldSuccessfullySendRequestCountEventUsingSend() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDocumentRequestEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending request count event
        Uni<Void> result = requestCountEventEmitter.send(testDocumentRequestEvent);

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDocumentRequestEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        assertThat(result)
            .as("Result should be a Uni<Void>")
            .isNotNull();
    }

    @Test
    @DisplayName("Should successfully send RequestCountEvent using sendAndForget() method")
    void shouldSuccessfullySendRequestCountEventUsingSendAndForget() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDocumentRequestEvent)).thenReturn(expectedKey);

        // When: Sending request count event with sendAndForget
        assertThatCode(() -> requestCountEventEmitter.sendAndForget(testDocumentRequestEvent))
            .as("sendAndForget should not throw any exceptions")
            .doesNotThrowAnyException();

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDocumentRequestEvent);
        verify(mockMutinyEmitter).sendMessageAndForget(any());
    }

    @Test
    @DisplayName("Should generate key using documentId for document request events")
    void shouldGenerateKeyUsingDocumentIdForDocumentRequestEvents() {
        // Given: RequestCountEvent with document request
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("pipe-doc-98765")
                .setOperation("WRITE")
                .setBytesTransferred(2048L)
                .setProcessingTimeMs(300L)
                .build())
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes("pipe-doc-98765".getBytes());
        when(mockKeyStrategy.getKey(event)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending document request event
        requestCountEventEmitter.send(event);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(event);
    }

    @Test
    @DisplayName("Should generate key using driveId for drive request events")
    void shouldGenerateKeyUsingDriveIdForDriveRequestEvents() {
        // Given: RequestCountEvent with drive request (no document request)
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setDriveId(67890L)
            .setDriveName("different-drive")
            .setCustomerId("customer-456")
            .setDriveRequest(RequestCountEvent.DriveRequestCount.newBuilder()
                .setOperation("DELETE")
                .setProcessingTimeMs(400L)
                .build())
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes("67890".getBytes());
        when(mockKeyStrategy.getKey(event)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending drive request event
        requestCountEventEmitter.send(event);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(event);
    }

    @Test
    @DisplayName("Should handle different RequestCountEvent types")
    void shouldHandleDifferentRequestCountEventTypes() {
        // Given: Different types of request count events
        RequestCountEvent documentRequestEvent = RequestCountEvent.newBuilder()
            .setDriveId(11111L)
            .setDriveName("drive-111")
            .setCustomerId("customer-111")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("pipe-doc-111")
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(100L)
                .build())
            .build();
        
        RequestCountEvent driveRequestEvent = RequestCountEvent.newBuilder()
            .setDriveId(22222L)
            .setDriveName("drive-222")
            .setCustomerId("customer-222")
            .setDriveRequest(RequestCountEvent.DriveRequestCount.newBuilder()
                .setOperation("UPDATE")
                .setProcessingTimeMs(200L)
                .build())
            .build();
        
        RequestCountEvent listRequestEvent = RequestCountEvent.newBuilder()
            .setDriveId(33333L)
            .setDriveName("drive-333")
            .setCustomerId("customer-333")
            .setListRequest(RequestCountEvent.ListRequestCount.newBuilder()
                .setPath("/test/path")
                .setResultCount(25)
                .setProcessingTimeMs(50L)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(any())).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending different request count events
        requestCountEventEmitter.send(documentRequestEvent);
        requestCountEventEmitter.send(driveRequestEvent);
        requestCountEventEmitter.send(listRequestEvent);

        // Then: Should handle all event types
        verify(mockKeyStrategy, times(3)).getKey(any());
        verify(mockMutinyEmitter, times(3)).sendMessage(any());
    }

    @Test
    @DisplayName("Should ensure document request events use same key as DocumentEvent with same documentId")
    void shouldEnsureDocumentRequestEventsUseSameKeyAsDocumentEventWithSameDocumentId() {
        // Given: RequestCountEvent with document request
        String documentId = "pipe-doc-consistent";
        
        RequestCountEvent requestEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(documentId)
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(150L)
                .build())
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes(documentId.getBytes());
        when(mockKeyStrategy.getKey(requestEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending document request event
        requestCountEventEmitter.send(requestEvent);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(requestEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        // This test verifies that the key strategy is called correctly.
        // The actual key generation logic is tested in KafkaKeyStrategyTest.
    }

    @Test
    @DisplayName("Should handle list request events")
    void shouldHandleListRequestEvents() {
        // Given: List request event
        RequestCountEvent listEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setListRequest(RequestCountEvent.ListRequestCount.newBuilder()
                .setPath("/test/path")
                .setResultCount(10)
                .setProcessingTimeMs(75L)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(listEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending list request event
        requestCountEventEmitter.send(listEvent);

        // Then: Should handle list request event
        verify(mockKeyStrategy).getKey(listEvent);
        verify(mockMutinyEmitter).sendMessage(any());
    }

    @Test
    @DisplayName("Should handle document request events with different operations")
    void shouldHandleDocumentRequestEventsWithDifferentOperations() {
        // Given: Document request events with different operations
        String documentId = "pipe-doc-operations";
        
        RequestCountEvent readEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(documentId)
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(100L)
                .build())
            .build();
        
        RequestCountEvent writeEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(documentId)
                .setOperation("WRITE")
                .setBytesTransferred(2048L)
                .setProcessingTimeMs(200L)
                .build())
            .build();
        
        RequestCountEvent deleteEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(documentId)
                .setOperation("DELETE")
                .setBytesTransferred(0L)
                .setProcessingTimeMs(50L)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(any())).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending different operation events
        requestCountEventEmitter.send(readEvent);
        requestCountEventEmitter.send(writeEvent);
        requestCountEventEmitter.send(deleteEvent);

        // Then: Should handle all operation types
        verify(mockKeyStrategy, times(3)).getKey(any());
        verify(mockMutinyEmitter, times(3)).sendMessage(any());
    }

    @Test
    @DisplayName("Should propagate exceptions from MutinyEmitter")
    void shouldPropagateExceptionsFromMutinyEmitter() {
        // Given: MutinyEmitter throws exception
        RuntimeException expectedException = new RuntimeException("Kafka send failed");
        when(mockKeyStrategy.getKey(testDocumentRequestEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any()))
            .thenReturn(Uni.createFrom().failure(expectedException));

        // When/Then: Should propagate the exception
        assertThatThrownBy(() -> requestCountEventEmitter.send(testDocumentRequestEvent).await().indefinitely())
            .as("Should propagate MutinyEmitter exceptions")
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Kafka send failed");
    }

    @Test
    @DisplayName("Should handle edge cases for RequestCountEvent")
    void shouldHandleEdgeCasesForRequestCountEvent() {
        // Test with zero driveId
        RequestCountEvent zeroDriveEvent = RequestCountEvent.newBuilder()
            .setDriveId(0L)
            .setDriveName("zero-drive")
            .setCustomerId("customer-zero")
            .setDriveRequest(RequestCountEvent.DriveRequestCount.newBuilder()
                .setOperation("CREATE")
                .setProcessingTimeMs(100L)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(zeroDriveEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());
        
        assertThatCode(() -> requestCountEventEmitter.send(zeroDriveEvent))
            .as("Should handle zero driveId")
            .doesNotThrowAnyException();
        
        // Test with empty documentId
        RequestCountEvent emptyDocEvent = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("")
                .setOperation("READ")
                .setBytesTransferred(0L)
                .setProcessingTimeMs(0L)
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(emptyDocEvent)).thenReturn(UUID.randomUUID());
        
        assertThatCode(() -> requestCountEventEmitter.sendAndForget(emptyDocEvent))
            .as("Should handle empty documentId")
            .doesNotThrowAnyException();
        
        verify(mockKeyStrategy, times(2)).getKey(any());
    }

    @Test
    @DisplayName("Should maintain consistent behavior across multiple sends")
    void shouldMaintainConsistentBehaviorAcrossMultipleSends() {
        // Given: Same request count event sent multiple times
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDocumentRequestEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending the same event multiple times
        requestCountEventEmitter.send(testDocumentRequestEvent);
        requestCountEventEmitter.send(testDocumentRequestEvent);
        requestCountEventEmitter.sendAndForget(testDocumentRequestEvent);

        // Then: Should call key strategy and MutinyEmitter consistently
        verify(mockKeyStrategy, times(3)).getKey(testDocumentRequestEvent);
        verify(mockMutinyEmitter, times(2)).sendMessage(any());
        verify(mockMutinyEmitter, times(1)).sendMessageAndForget(any());
    }
}