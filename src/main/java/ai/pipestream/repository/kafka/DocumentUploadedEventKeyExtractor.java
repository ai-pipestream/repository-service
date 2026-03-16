package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from DocumentUploadedEvent for Kafka partitioning.
 *
 * Uses the doc_id to derive a deterministic UUID for partition locality.
 */
@ApplicationScoped
public class DocumentUploadedEventKeyExtractor implements UuidKeyExtractor<DocumentUploadedEvent> {

    @Override
    public UUID extractKey(DocumentUploadedEvent event) {
        String docId = event.getDocId();
        if (docId == null || docId.isBlank()) {
            return UUID.randomUUID();
        }
        return UUID.nameUUIDFromBytes(docId.getBytes(StandardCharsets.UTF_8));
    }
}
