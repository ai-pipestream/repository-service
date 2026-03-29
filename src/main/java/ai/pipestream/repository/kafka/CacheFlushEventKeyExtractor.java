package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.v1.CacheFlushEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from CacheFlushEvent for Kafka partitioning.
 *
 * Uses the node_id to derive a deterministic UUID — the node_id uniquely
 * identifies the cached document that needs flushing from Redis to S3.
 */
@ApplicationScoped
public class CacheFlushEventKeyExtractor implements UuidKeyExtractor<CacheFlushEvent> {

    @Override
    public UUID extractKey(CacheFlushEvent event) {
        String nodeId = event.getNodeId();
        if (nodeId == null || nodeId.isBlank()) {
            return UUID.randomUUID();
        }
        return UUID.nameUUIDFromBytes(nodeId.getBytes(StandardCharsets.UTF_8));
    }
}
