package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DocumentEvent;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

/**
 * Emitter for DocumentEvent messages to Kafka.
 */
@ApplicationScoped
public class DocumentEventEmitter extends BaseRepoEmitter<DocumentEvent> {

    // No-args constructor for CDI
    public DocumentEventEmitter() {
        super(null, null);
    }

    @Inject
    public DocumentEventEmitter(@Channel("document-events") MutinyEmitter<DocumentEvent> delegate,
                              KafkaKeyStrategy keyStrategy) {
        super(delegate, keyStrategy);
    }
}