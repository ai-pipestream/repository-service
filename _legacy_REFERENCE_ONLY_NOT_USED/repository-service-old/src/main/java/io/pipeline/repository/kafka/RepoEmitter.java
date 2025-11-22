package io.pipeline.repository.kafka;

import com.google.protobuf.MessageLite;
import io.smallrye.mutiny.Uni;

/**
 * Interface for sending repository events to Kafka with automatic key generation.
 * 
 * This interface provides a clean abstraction over MutinyEmitter, handling:
 * - Automatic UUID key generation based on entity type
 * - Proper Kafka metadata configuration
 * - Apicurio Registry integration
 * 
 * @param <T> The protobuf message type
 */
public interface RepoEmitter<T extends MessageLite> {
    
    /**
     * Send a message to Kafka and return a Uni for async handling.
     * 
     * @param message The protobuf message to send
     * @return Uni that completes when the message is acknowledged
     */
    Uni<Void> send(T message);
    
    /**
     * Send a message to Kafka without waiting for acknowledgement (fire-and-forget).
     * 
     * @param message The protobuf message to send
     */
    void sendAndForget(T message);
}