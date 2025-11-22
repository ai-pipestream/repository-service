package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

/**
 * Emitter for DriveEvent messages to Kafka.
 */
@ApplicationScoped
public class DriveEventEmitter extends BaseRepoEmitter<DriveEvent> {

    // No-args constructor for CDI
    public DriveEventEmitter() {
        super(null, null);
    }

    @Inject
    public DriveEventEmitter(@Channel("drive-events") MutinyEmitter<DriveEvent> delegate,
                           KafkaKeyStrategy keyStrategy) {
        super(delegate, keyStrategy);
    }
}