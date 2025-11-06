package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DocumentEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for BaseRepoEmitter.
 * 
 * Tests the core functionality of automatic key generation and MutinyEmitter integration.
 */
@ExtendWith(MockitoExtension.class)
class BaseRepoEmitterTest {

    @Mock
    private MutinyEmitter<DocumentEvent> mockMutinyEmitter;
    
    @Mock
    private KafkaKeyStrategy mockKeyStrategy;
    
    private TestRepoEmitter testEmitter;
    private DocumentEvent testEvent;

    @BeforeEach
    void setUp() {
        testEmitter = new TestRepoEmitter(mockMutinyEmitter, mockKeyStrategy);
        
        testEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .build();
    }

    @Test
    @DisplayName("Should generate key and send message with proper metadata when calling send()")
    void shouldGenerateKeyAndSendMessageWithMetadata() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testEvent)).thenReturn(expectedKey);
        
        // Mock MutinyEmitter to capture the message
        AtomicReference<Message<DocumentEvent>> capturedMessage = new AtomicReference<>();
        when(mockMutinyEmitter.sendMessage(any(Message.class)))
            .thenAnswer(invocation -> {
                capturedMessage.set(invocation.getArgument(0));
                return Uni.createFrom().voidItem();
            });

        // When: Sending message
        Uni<Void> result = testEmitter.send(testEvent);

        // Then: Should call key strategy and MutinyEmitter with proper metadata
        verify(mockKeyStrategy).getKey(testEvent);
        verify(mockMutinyEmitter).sendMessage(any(Message.class));
        
        // Verify the captured message has correct metadata
        Message<DocumentEvent> sentMessage = capturedMessage.get();
        assertThat(sentMessage)
            .as("Sent message should not be null")
            .isNotNull();
        
        assertThat(sentMessage.getPayload())
            .as("Message payload should match the sent event")
            .isEqualTo(testEvent);
        
        // Verify metadata contains the expected key
        OutgoingKafkaRecordMetadata<?> metadata = sentMessage.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElse(null);
        assertThat(metadata)
            .as("Message should have OutgoingKafkaRecordMetadata")
            .isNotNull();
        
        assertThat(metadata.getKey())
            .as("Metadata should contain the generated key")
            .isEqualTo(expectedKey);
    }

    @Test
    @DisplayName("Should generate key and send message with proper metadata when calling sendAndForget()")
    void shouldGenerateKeyAndSendMessageWithMetadataForSendAndForget() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testEvent)).thenReturn(expectedKey);
        
        // Mock MutinyEmitter to capture the message
        AtomicReference<Message<DocumentEvent>> capturedMessage = new AtomicReference<>();
        doAnswer(invocation -> {
            capturedMessage.set(invocation.getArgument(0));
            return null;
        }).when(mockMutinyEmitter).sendMessageAndForget(any(Message.class));

        // When: Sending message with sendAndForget
        assertThatCode(() -> testEmitter.sendAndForget(testEvent))
            .as("sendAndForget should not throw any exceptions")
            .doesNotThrowAnyException();

        // Then: Should call key strategy and MutinyEmitter with proper metadata
        verify(mockKeyStrategy).getKey(testEvent);
        verify(mockMutinyEmitter).sendMessageAndForget(any(Message.class));
        
        // Verify the captured message has correct metadata
        Message<DocumentEvent> sentMessage = capturedMessage.get();
        assertThat(sentMessage)
            .as("Sent message should not be null")
            .isNotNull();
        
        assertThat(sentMessage.getPayload())
            .as("Message payload should match the sent event")
            .isEqualTo(testEvent);
        
        // Verify metadata contains the expected key
        OutgoingKafkaRecordMetadata<?> metadata = sentMessage.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElse(null);
        assertThat(metadata)
            .as("Message should have OutgoingKafkaRecordMetadata")
            .isNotNull();
        
        assertThat(metadata.getKey())
            .as("Metadata should contain the generated key")
            .isEqualTo(expectedKey);
    }

    @Test
    @DisplayName("Should handle different message types with appropriate key generation")
    void shouldHandleDifferentMessageTypesWithAppropriateKeyGeneration() {
        // Given: Different document events
        DocumentEvent event1 = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document1.pdf")
            .build();
        
        DocumentEvent event2 = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-456")
            .setDriveId(67890L)
            .setDriveName("different-drive")
            .setPath("/test/path/document2.pdf")
            .build();
        
        UUID key1 = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        
        when(mockKeyStrategy.getKey(event1)).thenReturn(key1);
        when(mockKeyStrategy.getKey(event2)).thenReturn(key2);
        
        when(mockMutinyEmitter.sendMessage(any(Message.class)))
            .thenReturn(Uni.createFrom().voidItem());

        // When: Sending different messages
        testEmitter.send(event1);
        testEmitter.send(event2);

        // Then: Should call key strategy for each message
        verify(mockKeyStrategy).getKey(event1);
        verify(mockKeyStrategy).getKey(event2);
        verify(mockMutinyEmitter, times(2)).sendMessage(any(Message.class));
    }

    @Test
    @DisplayName("Should propagate MutinyEmitter exceptions")
    void shouldPropagateMutinyEmitterExceptions() {
        // Given: MutinyEmitter throws exception
        RuntimeException expectedException = new RuntimeException("Kafka send failed");
        when(mockKeyStrategy.getKey(testEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any(Message.class)))
            .thenReturn(Uni.createFrom().failure(expectedException));

        // When/Then: Should propagate the exception
        assertThatThrownBy(() -> testEmitter.send(testEvent).await().indefinitely())
            .as("Should propagate MutinyEmitter exceptions")
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Kafka send failed");
    }

    @Test
    @DisplayName("Should handle null message gracefully")
    void shouldHandleNullMessageGracefully() {
        // Given: Null message
        when(mockKeyStrategy.getKey(null))
            .thenThrow(new IllegalArgumentException("Message cannot be null"));

        // When/Then: Should propagate the exception from key strategy
        assertThatThrownBy(() -> testEmitter.send(null))
            .as("Should propagate key strategy exceptions for null messages")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Message cannot be null");
    }

    @Test
    @DisplayName("Should create proper Message wrapper with metadata")
    void shouldCreateProperMessageWrapperWithMetadata() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.nameUUIDFromBytes("pipe-doc-123".getBytes());
        when(mockKeyStrategy.getKey(testEvent)).thenReturn(expectedKey);
        
        // Mock MutinyEmitter to capture the message
        AtomicReference<Message<DocumentEvent>> capturedMessage = new AtomicReference<>();
        when(mockMutinyEmitter.sendMessage(any(Message.class)))
            .thenAnswer(invocation -> {
                capturedMessage.set(invocation.getArgument(0));
                return Uni.createFrom().voidItem();
            });

        // When: Sending message
        testEmitter.send(testEvent);

        // Then: Verify the message wrapper is correct
        Message<DocumentEvent> sentMessage = capturedMessage.get();
        
        // Verify message structure
        assertThat(sentMessage.getPayload())
            .as("Message payload should be the original event")
            .isEqualTo(testEvent);
        
        // Verify metadata structure
        OutgoingKafkaRecordMetadata<?> metadata = sentMessage.getMetadata(OutgoingKafkaRecordMetadata.class)
            .orElse(null);
        assertThat(metadata)
            .as("Message should have OutgoingKafkaRecordMetadata")
            .isNotNull();
        
        assertThat(metadata.getKey())
            .as("Metadata key should match the generated key")
            .isEqualTo(expectedKey);
        
        // Verify metadata type
        assertThat(metadata.getKey())
            .as("Metadata key should be UUID type")
            .isInstanceOf(UUID.class);
    }

    /**
     * Test implementation of BaseRepoEmitter for testing purposes.
     */
    private static class TestRepoEmitter extends BaseRepoEmitter<DocumentEvent> {
        
        public TestRepoEmitter(MutinyEmitter<DocumentEvent> delegate, KafkaKeyStrategy keyStrategy) {
            super(delegate, keyStrategy);
        }
    }
}