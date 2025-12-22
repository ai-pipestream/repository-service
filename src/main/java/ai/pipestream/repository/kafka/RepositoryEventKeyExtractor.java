package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from RepositoryEvent for Kafka partitioning.
 *
 * Uses the document_id to derive a deterministic UUID for partition locality.
 */
@ApplicationScoped
public class RepositoryEventKeyExtractor implements UuidKeyExtractor<RepositoryEvent> {

    @Override
    public UUID extractKey(RepositoryEvent event) {
        String documentId = event.getDocumentId();
        if (documentId == null || documentId.isBlank()) {
            return UUID.randomUUID();
        }
        // Create deterministic UUID from document_id using UUID v5 (name-based)
        return UUID.nameUUIDFromBytes(documentId.getBytes(StandardCharsets.UTF_8));
    }
}
