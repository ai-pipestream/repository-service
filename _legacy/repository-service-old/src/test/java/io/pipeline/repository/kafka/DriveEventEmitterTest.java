package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
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
 * Unit tests for DriveEventEmitter.
 * 
 * Tests the drive event emitter functionality and integration with KafkaKeyStrategy.
 */
@ExtendWith(MockitoExtension.class)
class DriveEventEmitterTest {

    @Mock
    private MutinyEmitter<DriveEvent> mockMutinyEmitter;
    
    @Mock
    private KafkaKeyStrategy mockKeyStrategy;
    
    private DriveEventEmitter driveEventEmitter;
    private DriveEvent testDriveEvent;

    @BeforeEach
    void setUp() {
        driveEventEmitter = new DriveEventEmitter(mockMutinyEmitter, mockKeyStrategy);
        
        testDriveEvent = DriveEvent.newBuilder()
            .setEventId("event-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setCreated(DriveEvent.DriveCreated.newBuilder()
                .setBucketName("test-bucket")
                .setRegion("us-east-1")
                .setDescription("Test drive")
                .build())
            .build();
    }

    @Test
    @DisplayName("Should successfully send DriveEvent using send() method")
    void shouldSuccessfullySendDriveEventUsingSend() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDriveEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending drive event
        Uni<Void> result = driveEventEmitter.send(testDriveEvent);

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDriveEvent);
        verify(mockMutinyEmitter).sendMessage(any());
        
        assertThat(result)
            .as("Result should be a Uni<Void>")
            .isNotNull();
    }

    @Test
    @DisplayName("Should successfully send DriveEvent using sendAndForget() method")
    void shouldSuccessfullySendDriveEventUsingSendAndForget() {
        // Given: Mock key strategy returns specific UUID
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDriveEvent)).thenReturn(expectedKey);

        // When: Sending drive event with sendAndForget
        assertThatCode(() -> driveEventEmitter.sendAndForget(testDriveEvent))
            .as("sendAndForget should not throw any exceptions")
            .doesNotThrowAnyException();

        // Then: Should call key strategy and MutinyEmitter
        verify(mockKeyStrategy).getKey(testDriveEvent);
        verify(mockMutinyEmitter).sendMessageAndForget(any());
    }

    @Test
    @DisplayName("Should generate key using driveId for DriveEvent")
    void shouldGenerateKeyUsingDriveIdForDriveEvent() {
        // Given: DriveEvent with specific driveId
        DriveEvent event = DriveEvent.newBuilder()
            .setDriveId(98765L)
            .setDriveName("specific-drive")
            .setCustomerId("customer-987")
            .build();
        
        UUID expectedKey = UUID.nameUUIDFromBytes("98765".getBytes());
        when(mockKeyStrategy.getKey(event)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending drive event
        driveEventEmitter.send(event);

        // Then: Should call key strategy with the event
        verify(mockKeyStrategy).getKey(event);
    }

    @Test
    @DisplayName("Should handle different DriveEvent types")
    void shouldHandleDifferentDriveEventTypes() {
        // Given: Different types of drive events
        DriveEvent createdEvent = DriveEvent.newBuilder()
            .setDriveId(11111L)
            .setDriveName("created-drive")
            .setCustomerId("customer-111")
            .setCreated(DriveEvent.DriveCreated.newBuilder()
                .setBucketName("created-bucket")
                .setRegion("us-west-2")
                .setDescription("Created drive")
                .build())
            .build();
        
        DriveEvent updatedEvent = DriveEvent.newBuilder()
            .setDriveId(22222L)
            .setDriveName("updated-drive")
            .setCustomerId("customer-222")
            .setUpdated(DriveEvent.DriveUpdated.newBuilder()
                .setDescription("Updated drive")
                .build())
            .build();
        
        DriveEvent deletedEvent = DriveEvent.newBuilder()
            .setDriveId(33333L)
            .setDriveName("deleted-drive")
            .setCustomerId("customer-333")
            .setDeleted(DriveEvent.DriveDeleted.newBuilder()
                .setReason("No longer needed")
                .build())
            .build();
        
        when(mockKeyStrategy.getKey(any())).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending different drive events
        driveEventEmitter.send(createdEvent);
        driveEventEmitter.send(updatedEvent);
        driveEventEmitter.send(deletedEvent);

        // Then: Should handle all event types
        verify(mockKeyStrategy, times(3)).getKey(any());
        verify(mockMutinyEmitter, times(3)).sendMessage(any());
    }

    @Test
    @DisplayName("Should propagate exceptions from MutinyEmitter")
    void shouldPropagateExceptionsFromMutinyEmitter() {
        // Given: MutinyEmitter throws exception
        RuntimeException expectedException = new RuntimeException("Kafka connection failed");
        when(mockKeyStrategy.getKey(testDriveEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any()))
            .thenReturn(Uni.createFrom().failure(expectedException));

        // When/Then: Should propagate the exception
        assertThatThrownBy(() -> driveEventEmitter.send(testDriveEvent).await().indefinitely())
            .as("Should propagate MutinyEmitter exceptions")
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Kafka connection failed");
    }

    @Test
    @DisplayName("Should handle null DriveEvent gracefully")
    void shouldHandleNullDriveEventGracefully() {
        // Given: Null drive event
        when(mockKeyStrategy.getKey(null))
            .thenThrow(new IllegalArgumentException("DriveEvent cannot be null"));

        // When/Then: Should propagate the exception from key strategy
        assertThatThrownBy(() -> driveEventEmitter.send(null))
            .as("Should propagate key strategy exceptions for null events")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("DriveEvent cannot be null");
    }

    @Test
    @DisplayName("Should maintain consistent behavior across multiple sends")
    void shouldMaintainConsistentBehaviorAcrossMultipleSends() {
        // Given: Same drive event sent multiple times
        UUID expectedKey = UUID.randomUUID();
        when(mockKeyStrategy.getKey(testDriveEvent)).thenReturn(expectedKey);
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());

        // When: Sending the same event multiple times
        driveEventEmitter.send(testDriveEvent);
        driveEventEmitter.send(testDriveEvent);
        driveEventEmitter.sendAndForget(testDriveEvent);

        // Then: Should call key strategy and MutinyEmitter consistently
        verify(mockKeyStrategy, times(3)).getKey(testDriveEvent);
        verify(mockMutinyEmitter, times(2)).sendMessage(any());
        verify(mockMutinyEmitter, times(1)).sendMessageAndForget(any());
    }

    @Test
    @DisplayName("Should handle edge cases for DriveEvent")
    void shouldHandleEdgeCasesForDriveEvent() {
        // Test with zero driveId
        DriveEvent zeroDriveEvent = DriveEvent.newBuilder()
            .setDriveId(0L)
            .setDriveName("zero-drive")
            .setCustomerId("customer-zero")
            .build();
        
        when(mockKeyStrategy.getKey(zeroDriveEvent)).thenReturn(UUID.randomUUID());
        when(mockMutinyEmitter.sendMessage(any())).thenReturn(Uni.createFrom().voidItem());
        
        assertThatCode(() -> driveEventEmitter.send(zeroDriveEvent))
            .as("Should handle zero driveId")
            .doesNotThrowAnyException();
        
        // Test with empty drive name
        DriveEvent emptyNameEvent = DriveEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("")
            .setCustomerId("customer-empty")
            .build();
        
        when(mockKeyStrategy.getKey(emptyNameEvent)).thenReturn(UUID.randomUUID());
        
        assertThatCode(() -> driveEventEmitter.sendAndForget(emptyNameEvent))
            .as("Should handle empty drive name")
            .doesNotThrowAnyException();
        
        verify(mockKeyStrategy, times(2)).getKey(any());
    }
}