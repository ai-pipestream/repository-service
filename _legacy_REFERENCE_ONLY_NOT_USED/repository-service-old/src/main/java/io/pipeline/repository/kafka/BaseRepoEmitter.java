package io.pipeline.repository.kafka;

import com.google.protobuf.MessageLite;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Base implementation of RepoEmitter that handles MutinyEmitter integration.
 * 
 * This class provides the common functionality for:
 * - Automatic UUID key generation
 * - Kafka metadata configuration
 * - Message wrapping for MutinyEmitter
 * 
 * @param <T> The protobuf message type
 */
public abstract class BaseRepoEmitter<T extends MessageLite> implements RepoEmitter<T> {
    
    private static final Logger LOG = Logger.getLogger(BaseRepoEmitter.class);
    
    protected final MutinyEmitter<T> delegate;
    protected final KafkaKeyStrategy keyStrategy;
    
    protected BaseRepoEmitter(MutinyEmitter<T> delegate, KafkaKeyStrategy keyStrategy) {
        this.delegate = delegate;
        this.keyStrategy = keyStrategy;
    }
    
    @Override
    public Uni<Void> send(T message) {
        UUID key = keyStrategy.getKey(message);
        LOG.debugf("Sending message with key: %s, type: %s", key, message.getClass().getSimpleName());
        
        OutgoingKafkaRecordMetadata<UUID> metadata = OutgoingKafkaRecordMetadata.<UUID>builder()
            .withKey(key)
            .build();
        
        return delegate.sendMessage(Message.of(message).addMetadata(metadata));
    }
    
    @Override
    public void sendAndForget(T message) {
        UUID key = keyStrategy.getKey(message);
        LOG.debugf("Sending message (fire-and-forget) with key: %s, type: %s", key, message.getClass().getSimpleName());
        
        OutgoingKafkaRecordMetadata<UUID> metadata = OutgoingKafkaRecordMetadata.<UUID>builder()
            .withKey(key)
            .build();
        
        delegate.sendMessageAndForget(Message.of(message).addMetadata(metadata));
    }
}