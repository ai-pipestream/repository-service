package ai.pipestream.repository.consumer;

import ai.pipestream.repository.entity.Node;
import ai.pipestream.repository.filesystem.RepositoryEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.UUID;

import java.time.Instant;

/**
 * Kafka consumer for repository events.
 * Persists metadata to the database based on event type.
 */
@ApplicationScoped
public class MetadataIndexer {

    private static final Logger LOG = Logger.getLogger(MetadataIndexer.class);

    @Incoming("repository-events")
    @Blocking // Using blocking for now as Panache reactive might need more setup, or we can
              // use @Transactional
    @Transactional
    public void consume(Record<UUID, RepositoryEvent> record) {
        RepositoryEvent event = record.value();
        LOG.infof("Received event: %s type: %s", event.getEventId(), event.getOperationCase());

        if (event.hasCreated()) {
            handleCreated(event);
        } else if (event.hasUpdated()) {
            handleUpdated(event);
        } else if (event.hasDeleted()) {
            handleDeleted(event);
        }
    }

    private void handleCreated(RepositoryEvent event) {
        String docId = event.getDocumentId();
        RepositoryEvent.Created created = event.getCreated();
        String s3Key = created.getS3Key();

        // Extract drive from s3Key or context if available.
        // For now, we might need to lookup drive based on connector or bucket.
        // Assuming a default drive or lookup logic needs to be in place.
        // Since we don't have the drive info in the event directly (except maybe in
        // s3Key),
        // we'll need to infer it or fetch it.

        // TODO: Robust Drive lookup. For now, finding by s3Key prefix or creating a
        // placeholder if needed.
        // This is a simplification. In a real scenario, we'd likely have the driveId in
        // the event or look it up.

        Node node = new Node();
        node.nodeId = docId;
        node.name = docId; // Default name
        node.s3Key = s3Key;
        node.sizeBytes = created.getSize();
        node.s3Etag = created.getContentHash(); // Using content hash as etag proxy
        node.createdAt = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        node.updatedAt = node.createdAt;
        node.status = "ACTIVE";

        // Persist
        node.persist();
        LOG.infof("Persisted metadata for node: %s", docId);
    }

    private void handleUpdated(RepositoryEvent event) {
        // TODO: Implement update logic
    }

    private void handleDeleted(RepositoryEvent event) {
        // TODO: Implement delete logic
    }
}
