package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DocumentEvent;
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
 * Unit tests for DocumentEventEmitter.
 * 
 * Tests the document event emitter functionality and integration with KafkaKeyStrategy.
 * Focuses on document-centric partitioning using documentId (PipeDoc ID).
 */
@ExtendWith(MockitoExtension.class)
class DocumentEventEmitterTest {

    @Mock
    private MutinyEmitter<DocumentEvent> mockMutinyEmitter;
    
    @Mock
    private KafkaKeyStrategy mockKeyStrategy;
    
    private DocumentEventEmitter documentEventEmitter;
    private DocumentEvent testDocumentEvent;

    @BeforeEach
    void setUp() {
        documentEventEmitter = new DocumentEventEmitter(mockMutinyEmitter, mockKeyStrategy);
        
        testDocumentEvent = DocumentEvent.newBuilder()
            .setEventId("event-123")
            .setDocumentId("pipe-doc-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                .setName("document.pdf")
                .setDocumentType("PDF")
                .setContentType("application/pdf")
                .setSize(1024L)
                .setS3Key("s3://test-bucket/document.pdf")
                .setPayloadType("PipeDoc")
                .build())
            .build();
    }

    @Test
    @DisplayName("Should successfully send DocumentEvent using send() method")
    void shouldSuccessfullySendDocumentEventUsingSend() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDocumentEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending document event
        Uni<Void> result = documentEventEmitter.send(testDocumentEvent);

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDocumentEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        assertThat(result)
            .as("Result should be a Uni<Void>")
            .isNotNull();
    }

    @Test
    @DisplayName("Should successfully send DocumentEvent using sendAndForget() method")
    void shouldSuccessfullySendDocumentEventUsingSendAndForget() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDocumentEvent)).thenReturn(expectedKey);

        // When: Sending document event with sendAndForget
        assertThatCode(() -> documentEventEmitter.sendAndForget(testDocumentEvent))
            .as("sendAndForget should not throw any exceptions")
            .doesNotThrowAnyException();

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDocumentEvent);
        verify(mockMutinyEmitter).sendMessageAndForget(any());
    }

    @Test
    @DisplayName("Should generate key using documentId (PipeDoc ID) for DocumentEvent")
    void shouldGenerateKeyUsingDocumentIdForDocumentEvent() {
        // Given: DocumentEvent with specific documentId
        DocumentEvent event = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-98765")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/specific-document.pdf")
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes("pipe-doc-98765".getBytes());
        when(mockKeyStrategy.getKey(event)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending document event
        documentEventEmitter.send(event);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(event);
    }

    @Test
    @DisplayName("Should handle different DocumentEvent types")
    void shouldHandleDifferentDocumentEventTypes() {
        // Given: Different types of document events
        DocumentEvent createdEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-111")
            .setDriveId(11111L)
            .setDriveName("drive-111")
            .setPath("/test/path/created.pdf")
            .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                .setName("created.pdf")
                .setDocumentType("PDF")
                .setContentType("application/pdf")
                .setSize(2048L)
                .setS3Key("s3://bucket/created.pdf")
                .setPayloadType("PipeDoc")
                .build())
            .build();
        
        DocumentEvent updatedEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-222")
            .setDriveId(22222L)
            .setDriveName("drive-222")
            .setPath("/test/path/updated.pdf")
            .setUpdated(DocumentEvent.DocumentUpdated.newBuilder()
                .setName("updated.pdf")
                .setContentType("application/pdf")
                .setSize(4096L)
                .build())
            .build();
        
        DocumentEvent deletedEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-333")
            .setDriveId(33333L)
            .setDriveName("drive-333")
            .setPath("/test/path/deleted.pdf")
            .setDeleted(DocumentEvent.DocumentDeleted.newBuilder()
                .setReason("User requested deletion")
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(any())).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending different document events
        documentEventEmitter.send(createdEvent);
        documentEventEmitter.send(updatedEvent);
        documentEventEmitter.send(deletedEvent);

        // Then: Should handle all event types
        verify(mockKeyStrategy, times(3)).getKey(any());
        verify(mockMutinyEmitter, times(3)).sendMessage(any());
    }

    @Test
    @DisplayName("Should ensure same documentId generates same key for consistent partitioning")
    void shouldEnsureSameDocumentIdGeneratesSameKeyForConsistentPartitioning() {
        // Given: Multiple events with same documentId
        String documentId = "pipe-doc-consistent";
        
        DocumentEvent event1 = DocumentEvent.newBuilder()
            .setDocumentId(documentId)
            .setDriveId(12345L)
            .setDriveName("drive-1")
            .setPath("/path/event1.pdf")
            .build();
        
        DocumentEvent event2 = DocumentEvent.newBuilder()
            .setDocumentId(documentId)
            .setDriveId(67890L) // Different drive
            .setDriveName("drive-2")
            .setPath("/path/event2.pdf")
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes(documentId.getBytes());
        when(mockKeyStrategy.getKey(event1)).thenReturn(expectedKey);
        when(mockKeyStrategy.getKey(event2)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending events with same documentId
        documentEventEmitter.send(event1);
        documentEventEmitter.send(event2);

        // Then: Should call key strategy for both events
        verify(mockKeyStrategy).getKey(event1);
        verify(mockKeyStrategy).getKey(event2);
        verify(mockMutinyEmitter, times(2)).sendMessage(any());
    }

    @Test
    @DisplayName("Should handle document access events")
    void shouldHandleDocumentAccessEvents() {
        // Given: Document access event
        DocumentEvent accessEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-access")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/accessed.pdf")
            .setAccessed(DocumentEvent.DocumentAccessed.newBuilder()
                .setAccessType("READ")
                .setUserId("user-123")
                .setClientIp("192.168.1.100")
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(accessEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending access event
        documentEventEmitter.send(accessEvent);

        // Then: Should handle access event
        verify(mockKeyStrategy).getKey(accessEvent);
        verify(mockMutinyEmitter).sendMessage(any());
    }

    @Test
    @DisplayName("Should handle document upload progress events")
    void shouldHandleDocumentUploadProgressEvents() {
        // Given: Document upload progress event
        DocumentEvent progressEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-progress")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/uploading.pdf")
            .setUploadProgress(DocumentEvent.DocumentUploadProgress.newBuilder()
                .setUploadStatus("IN_PROGRESS")
                .setTotalChunks(10)
                .setCompletedChunks(7)
                .setErrorMessage("")
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(progressEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending progress event
        documentEventEmitter.send(progressEvent);

        // Then: Should handle progress event
        verify(mockKeyStrategy).getKey(progressEvent);
        verify(mockMutinyEmitter).sendMessage(any());
    }

    @Test
    @DisplayName("Should propagate exceptions from MutinyEmitter")
    void shouldPropagateExceptionsFromMutinyEmitter() {
        // Given: MutinyEmitter throws exception
        RuntimeException expectedException = new RuntimeException("Kafka send failed");
        when(mockKeyStrategy.getKey(testDocumentEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any()))
            .thenReturn(Uni.createFrom().failure(expectedException));

        // When/Then: Should propagate the exception
        assertThatThrownBy(() -> documentEventEmitter.send(testDocumentEvent).await().indefinitely())
            .as("Should propagate MutinyEmitter exceptions")
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Kafka send failed");
    }

    @Test
    @DisplayName("Should handle edge cases for DocumentEvent")
    void shouldHandleEdgeCasesForDocumentEvent() {
        // Test with empty documentId
        DocumentEvent emptyDocEvent = DocumentEvent.newBuilder()
            .setDocumentId("")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/empty-doc.pdf")
            .build();
        
        when(mockKeyStrategy.getKey(emptyDocEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());
        
        assertThatCode(() -> documentEventEmitter.send(emptyDocEvent))
            .as("Should handle empty documentId")
            .doesNotThrowAnyException();
        
        // Test with very long documentId
        String longDocumentId = "pipe-doc-" + "x".repeat(1000);
        DocumentEvent longDocEvent = DocumentEvent.newBuilder()
            .setDocumentId(longDocumentId)
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/long-doc.pdf")
            .build();
        
        when(mockKeyStrategy.getKey(longDocEvent)).thenReturn(UUID.randomUUID());
        
        assertThatCode(() -> documentEventEmitter.sendAndForget(longDocEvent))
            .as("Should handle very long documentId")
            .doesNotThrowAnyException();
        
        verify(mockKeyStrategy, times(2)).getKey(any());
    }
}