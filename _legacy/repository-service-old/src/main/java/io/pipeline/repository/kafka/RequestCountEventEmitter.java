package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.RequestCountEvent;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

/**
 * Emitter for RequestCountEvent messages to Kafka.
 */
@ApplicationScoped
public class RequestCountEventEmitter extends BaseRepoEmitter<RequestCountEvent> {

    // No-args constructor for CDI
    public RequestCountEventEmitter() {
        super(null, null);
    }

    @Inject
    public RequestCountEventEmitter(@Channel("request-count-events") MutinyEmitter<RequestCountEvent> delegate,
                                  KafkaKeyStrategy keyStrategy) {
        super(delegate, keyStrategy);
    }
}