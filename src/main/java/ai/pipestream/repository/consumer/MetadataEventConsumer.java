package ai.pipestream.repository.consumer;

import ai.pipestream.repository.filesystem.RepositoryEvent;
import ai.pipestream.repository.service.MetadataService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Kafka Consumer for Repository Events.
 * Listens for events and triggers metadata persistence.
 */
@ApplicationScoped
public class MetadataEventConsumer {

    private static final Logger LOG = Logger.getLogger(MetadataEventConsumer.class);

    @Inject
    MetadataService metadataService;

    @Incoming("repository-events-in")
    public Uni<Void> consume(io.smallrye.reactive.messaging.kafka.Record<UUID, RepositoryEvent> record) {
        RepositoryEvent event = record.value();
        LOG.infof("Received RepositoryEvent: %s", event.getEventId());

        return metadataService.persistMetadata(event)
                .onItem().invoke(doc -> {
                    if (doc != null) {
                        LOG.infof("Successfully persisted metadata for doc: %s", doc.documentId);
                    } else {
                        LOG.debug("Skipped metadata persistence (not a Created event)");
                    }
                })
                .onFailure()
                .invoke(th -> LOG.errorf(th, "Failed to persist metadata for event: %s", event.getEventId()))
                .replaceWithVoid();
    }
}
