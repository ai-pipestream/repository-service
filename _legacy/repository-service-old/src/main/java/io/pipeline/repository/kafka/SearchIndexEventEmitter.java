package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

/**
 * Emitter for SearchIndexEvent messages to Kafka.
 */
@ApplicationScoped
public class SearchIndexEventEmitter extends BaseRepoEmitter<SearchIndexEvent> {

    // No-args constructor for CDI
    public SearchIndexEventEmitter() {
        super(null, null);
    }

    @Inject
    public SearchIndexEventEmitter(@Channel("search-index-events") MutinyEmitter<SearchIndexEvent> delegate,
                                 KafkaKeyStrategy keyStrategy) {
        super(delegate, keyStrategy);
    }
}