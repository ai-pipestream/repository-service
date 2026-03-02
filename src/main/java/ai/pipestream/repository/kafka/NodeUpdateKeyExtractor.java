package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.filesystem.v1.NodeUpdateNotification;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from NodeUpdateNotification for Kafka partitioning.
 *
 * Uses the node's document_id to derive a deterministic UUID for partition locality.
 */
@ApplicationScoped
public class NodeUpdateKeyExtractor implements UuidKeyExtractor<NodeUpdateNotification> {

    @Override
    public UUID extractKey(NodeUpdateNotification event) {
        if (event == null || !event.hasNode() || event.getNode().getDocumentId().isBlank()) {
            return UUID.nameUUIDFromBytes("unknown-node".getBytes(StandardCharsets.UTF_8));
        }
        return UUID.nameUUIDFromBytes(event.getNode().getDocumentId().getBytes(StandardCharsets.UTF_8));
    }
}
