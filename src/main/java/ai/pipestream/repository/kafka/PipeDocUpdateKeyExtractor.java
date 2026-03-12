package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from PipeDocUpdateNotification for Kafka partitioning.
 *
 * Uses the storage_id (node UUID) for deterministic partition locality.
 */
@ApplicationScoped
public class PipeDocUpdateKeyExtractor implements UuidKeyExtractor<PipeDocUpdateNotification> {

    @Override
    public UUID extractKey(PipeDocUpdateNotification event) {
        String storageId = event.getStorageId();
        if (storageId == null || storageId.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(storageId);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(storageId.getBytes(StandardCharsets.UTF_8));
        }
    }
}
