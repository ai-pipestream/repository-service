package ai.pipestream.repository.kafka;

import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock RepositoryEventEmitter for tests.
 * Records emitted events for verification without requiring Kafka.
 */
@Mock
@ApplicationScoped
public class MockRepositoryEventEmitter extends RepositoryEventEmitter {

    private static final Logger LOG = Logger.getLogger(MockRepositoryEventEmitter.class);

    private final List<EmittedEvent> emittedEvents = new ArrayList<>();

    @Override
    public void emitCreated(String docId, String accountId, String s3Key, String pipedocS3Key,
                            long sizeBytes, String contentHash, String bucket, String versionId,
                            String requestId, String connectorId) {
        LOG.infof("Mock emitCreated: docId=%s, s3Key=%s", docId, s3Key);
        emittedEvents.add(new EmittedEvent("CREATED", docId, accountId, s3Key));
    }

    @Override
    public void emitUpdated(String docId, String accountId, String s3Key, long sizeBytes,
                            String contentHash, String bucket, String newVersionId,
                            String previousVersionId, String requestId, String connectorId) {
        LOG.infof("Mock emitUpdated: docId=%s, s3Key=%s", docId, s3Key);
        emittedEvents.add(new EmittedEvent("UPDATED", docId, accountId, s3Key));
    }

    @Override
    public void emitDeleted(String docId, String accountId, String reason, boolean purged,
                            String requestId, String connectorId) {
        LOG.infof("Mock emitDeleted: docId=%s, purged=%s", docId, purged);
        emittedEvents.add(new EmittedEvent("DELETED", docId, accountId, null));
    }

    public List<EmittedEvent> getEmittedEvents() {
        return new ArrayList<>(emittedEvents);
    }

    public void clear() {
        emittedEvents.clear();
    }

    public record EmittedEvent(String type, String docId, String accountId, String s3Key) {}
}
