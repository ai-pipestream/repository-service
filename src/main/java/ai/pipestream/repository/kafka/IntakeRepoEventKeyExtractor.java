package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.events.v1.IntakeRepoEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts a deterministic UUID key from IntakeRepoEvent for Kafka partitioning.
 *
 * The document id is used as the partition key source so all events for the same
 * document stay ordered and map to the same partition.
 */
@ApplicationScoped
public class IntakeRepoEventKeyExtractor implements UuidKeyExtractor<IntakeRepoEvent> {

    @Override
    public UUID extractKey(IntakeRepoEvent event) {
        if (event == null || event.getDocId() == null || event.getDocId().isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(event.getDocId());
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(event.getDocId().getBytes(StandardCharsets.UTF_8));
        }
    }
}
