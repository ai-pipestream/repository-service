package io.pipeline.repository.managers;

import io.pipeline.repository.entity.Drive;
import io.pipeline.repository.entity.Node;
import io.pipeline.repository.entity.NodeType;
import io.pipeline.repository.exception.*;
import io.pipeline.repository.services.EventPublisher;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * Manages Node metadata, database operations, and Kafka events.
 * Handles post-upload operations and CRUD functionality.
 */
@ApplicationScoped
public class NodeManager {

    private static final Logger LOG = Logger.getLogger(NodeManager.class);

    @Inject
    EventPublisher eventPublisher;
    
    @Inject
    Drive.DriveService driveService;

    /**
     * Create Node metadata after successful upload.
     */
    @Transactional
    public Uni<Node> createNodeMetadata(String nodeId, String driveName, String connectorId,
                                       String path, String name, String contentType,
                                       long size, String s3Key, String s3VersionId,
                                       String protobufType, Map<String, String> metadata,
                                       String contentHash) {

        return Uni.createFrom().item(() -> {
            LOG.infof("Creating node metadata: nodeId=%s, drive=%s, s3Key=%s", nodeId, driveName, s3Key);

            // Look up drive
            Drive drive = driveService.findByName(driveName);
            if (drive == null) {
                throw new DriveNotFoundException(driveName);
            }

            // Look up node type (default to FILE)
            NodeType nodeType = NodeType.findByCode("FILE");
            if (nodeType == null) {
                throw new DatabaseOperationException("createNodeMetadata", "NodeType", "FILE type not found");
            }

            // Create node entity
            Node node = new Node();
            node.documentId = nodeId;
            node.driveId = drive.id;
            node.name = name;
            node.nodeTypeId = nodeType.id;
            node.parentId = null; // Root level for now
            node.path = path != null ? path : ("/" + name);
            node.contentType = contentType;
            node.size = size;
            node.s3Key = s3Key;
            node.s3VersionId = s3VersionId;
            node.contentHash = contentHash;
            node.createdAt = OffsetDateTime.now();
            node.updatedAt = OffsetDateTime.now();

            // Store metadata as JSON
            if (metadata != null && !metadata.isEmpty()) {
                try {
                    node.metadata = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(metadata);
                } catch (Exception e) {
                    LOG.warnf("Failed to serialize metadata for node %s", nodeId);
                    node.metadata = "{}";
                }
            } else {
                node.metadata = "{}";
            }

            // Set protobuf-specific fields if available
            if (protobufType != null) {
                node.payloadType = protobufType;
                node.serviceType = "ProtobufService";
            }

            // Save to database
            node.persist();

            LOG.infof("Node metadata created: id=%d, documentId=%s", node.id, nodeId);

            // Emit Kafka event (fire-and-forget)
            try {
                eventPublisher.publishDocumentCreatedAndForget(
                    nodeId, driveName, drive.id, node.path, name, contentType,
                    size, s3Key, protobufType != null ? protobufType : "unknown",
                    contentHash != null ? contentHash : "", s3VersionId, drive.accountId, connectorId
                );
            } catch (Exception e) {
                LOG.warnf("Failed to emit document created event for %s: %s", nodeId, e.getMessage());
            }

            return node;
        });
    }

    /**
     * Get Node by document ID with optional payload.
     */
    public Uni<NodeResult> getNode(String nodeId, boolean includePayload) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Getting node: nodeId=%s, includePayload=%s", nodeId, includePayload);

            Node node = Node.findByDocumentId(nodeId);
            if (node == null) {
                throw new DocumentNotFoundException(nodeId);
            }

            Drive drive = Drive.findById(node.driveId);
            if (drive == null) {
                throw new DriveNotFoundException(node.driveId.toString());
            }

            return new NodeResult(node, drive, null); // Payload loading handled elsewhere
        }).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Update Node metadata.
     */
    @Transactional
    public Uni<Node> updateNodeMetadata(String nodeId, String name, String contentType,
                                       Map<String, String> metadata) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Updating node metadata: nodeId=%s", nodeId);

            Node node = Node.findByDocumentId(nodeId);
            if (node == null) {
                throw new DocumentNotFoundException(nodeId);
            }

            // Update fields
            if (name != null) {
                node.name = name;
            }
            if (contentType != null) {
                node.contentType = contentType;
            }
            if (metadata != null) {
                try {
                    node.metadata = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(metadata);
                } catch (Exception e) {
                    LOG.warnf("Failed to serialize metadata for node %s", nodeId);
                }
            }

            node.updatedAt = OffsetDateTime.now();
            node.persist();

            // Emit update event
            Drive drive = Drive.findById(node.driveId);
            try {
                eventPublisher.publishDocumentUpdatedAndForget(
                    nodeId, drive.name, drive.id, node.path, node.name, node.contentType,
                    node.size, node.payloadType, "", node.s3VersionId, "", drive.accountId
                );
            } catch (Exception e) {
                LOG.warnf("Failed to emit document updated event for %s: %s", nodeId, e.getMessage());
            }

            return node;
        });
    }

    /**
     * Delete Node and emit event.
     */
    @Transactional
    public Uni<Boolean> deleteNode(String nodeId, String reason) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Deleting node: nodeId=%s, reason=%s", nodeId, reason);

            Node node = Node.findByDocumentId(nodeId);
            if (node == null) {
                return false; // Already deleted
            }

            Drive drive = Drive.findById(node.driveId);
            String driveName = drive != null ? drive.name : "unknown";
            long driveId = drive != null ? drive.id : 0L;
            String path = node.path;

            // Delete from database
            node.delete();
            boolean deleted = true;

            if (deleted) {
                // Emit delete event
                try {
                    eventPublisher.publishDocumentDeletedAndForget(nodeId, driveName, driveId, path, reason);
                } catch (Exception e) {
                    LOG.warnf("Failed to emit document deleted event for %s: %s", nodeId, e.getMessage());
                }
            }

            return deleted;
        });
    }

    /**
     * Search nodes by criteria.
     */
    public Uni<List<Node>> searchNodes(String driveName, String query, String connectorId,
                                      int pageSize, String pageToken) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Searching nodes: drive=%s, query=%s, connector=%s", driveName, query, connectorId);

            // Basic search implementation
            if (driveName != null) {
                Drive drive = driveService.findByName(driveName);
                if (drive == null) {
                    throw new DriveNotFoundException(driveName);
                }
                return Node.find("driveId", drive.id).page(0, pageSize).list();
            } else {
                return Node.findAll().page(0, pageSize).list();
            }
        });
    }

    /**
     * Result containing Node and related data.
     */
    public static class NodeResult {
        public final Node node;
        public final Drive drive;
        public final byte[] payload;

        public NodeResult(Node node, Drive drive, byte[] payload) {
            this.node = node;
            this.drive = drive;
            this.payload = payload;
        }
    }
}