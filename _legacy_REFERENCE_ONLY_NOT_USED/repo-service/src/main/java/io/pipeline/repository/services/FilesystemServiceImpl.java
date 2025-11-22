package io.pipeline.repository.services;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pipeline.repository.filesystem.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;
import io.pipeline.repository.managers.UploadManager;
import io.pipeline.repository.managers.NodeManager;
import io.pipeline.repository.managers.PathManager;
import io.pipeline.repository.managers.DriveManager;
import io.pipeline.repository.service.DocumentService;
import io.pipeline.repository.services.S3Service;
import io.pipeline.repository.exception.*;
import io.pipeline.repository.filesystem.upload.*;
import io.pipeline.repository.entity.*;
import com.google.protobuf.Timestamp;
import com.google.protobuf.InvalidProtocolBufferException;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.transaction.Transactional;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Clean implementation of FilesystemService with database integration.
 */
@GrpcService
@Singleton
@Default
public class FilesystemServiceImpl extends MutinyFilesystemServiceGrpc.FilesystemServiceImplBase {

    private static final Logger LOG = Logger.getLogger(FilesystemServiceImpl.class);

    @Inject
    UploadManager uploadManager;

    @Inject
    NodeManager nodeManager;

    @Inject
    PathManager pathManager;

    @Inject
    S3Service s3Service;
    
    @Inject
    io.pipeline.repository.entity.Drive.DriveService driveService;

    @Inject
    DriveManager driveManager;

    @Override
    public Uni<io.pipeline.repository.filesystem.Node> getNode(GetNodeRequest request) {
        LOG.infof("Getting node: documentId=%s, drive=%s, includePayload=%s",
            request.getDocumentId(), request.getDrive(), request.getIncludePayload());

        // Use NodeManager to get node
        return nodeManager.getNode(request.getDocumentId(), request.getIncludePayload())
            .flatMap(nodeResult -> {
                if (request.getIncludePayload() && nodeResult.payload == null) {
                    // Need to load payload from S3
                    return Uni.createFrom().item(() -> {
                        try {
                            // Use full S3 key directly
                            byte[] payloadBytes = s3Service.getObject(nodeResult.drive.bucketName, nodeResult.node.s3Key);

                            LOG.infof("Hydrated payload for node: documentId=%s, size=%d bytes",
                                request.getDocumentId(), payloadBytes.length);

                            return new NodeManager.NodeResult(nodeResult.node, nodeResult.drive, payloadBytes);
                        } catch (Exception e) {
                            LOG.errorf(e, "Failed to hydrate payload for node %s", request.getDocumentId());
                            throw new RuntimeException("Failed to retrieve payload from S3: " + e.getMessage(), e);
                        }
                    }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
                } else {
                    return Uni.createFrom().item(nodeResult);
                }
            })
            .map(nodeResult -> {
                // Convert to gRPC Node
                io.pipeline.repository.filesystem.Node.Builder nodeBuilder =
                    io.pipeline.repository.filesystem.Node.newBuilder()
                        .setId(nodeResult.node.id)
                        .setDocumentId(nodeResult.node.documentId)
                        .setDriveId(nodeResult.node.driveId)
                        .setName(nodeResult.node.name)
                        .setNodeTypeId(nodeResult.node.nodeTypeId)
                        .setParentId(nodeResult.node.parentId != null ? nodeResult.node.parentId : 0L)
                        .setPath(nodeResult.node.path)
                        .setContentType(nodeResult.node.contentType)
                        .setSizeBytes(nodeResult.node.size != null ? nodeResult.node.size : 0L)
                        .setS3Key(nodeResult.node.s3Key != null ? nodeResult.node.s3Key : "")
                        .setCreatedAt(convertTimestamp(nodeResult.node.createdAt))
                        .setUpdatedAt(convertTimestamp(nodeResult.node.updatedAt))
                        .setMetadata(nodeResult.node.metadata != null ? nodeResult.node.metadata : "")
                        .setType(convertNodeType(nodeResult.node.nodeTypeId))
                    .setServiceType(nodeResult.node.serviceType != null ? nodeResult.node.serviceType : "")
                    .setPayloadType(nodeResult.node.payloadType != null ? nodeResult.node.payloadType : "");

                // Add payload if requested and available
                if (request.getIncludePayload() && nodeResult.payload != null) {
                    try {
                        com.google.protobuf.Any payload = com.google.protobuf.Any.parseFrom(nodeResult.payload);
                        nodeBuilder.setPayload(payload);
                    } catch (Exception e) {
                        LOG.errorf(e, "Failed to parse payload for node %s", request.getDocumentId());
                        throw new RuntimeException("Failed to parse payload", e);
                    }
                }

                return nodeBuilder.build();
            })
            .onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error getting node: documentId=%s", request.getDocumentId());
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal error getting node").withCause(e));
            }
        });
    }

    @Override
    public Uni<io.pipeline.repository.filesystem.Node> getNodeByPath(GetNodeByPathRequest request) {
        LOG.infof("Getting node by path: drive=%s, connector=%s, path=%s",
            request.getDrive(), request.getConnectorId(), request.getPath());

        return Uni.createFrom().item(() -> {
            // Look up drive
            io.pipeline.repository.entity.Drive drive = driveService.findByName(request.getDrive());
            if (drive == null) {
                throw new DriveNotFoundException(request.getDrive());
            }

            // Extract directory and filename from path
            String fullPath = request.getPath();
            String fileName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
            String dirPath = fullPath.substring(0, fullPath.lastIndexOf('/'));

            // Build S3 key pattern: connectors/{connector}/{dirPath}/{uuid}.pb
            String s3KeyPattern = "connectors/" + request.getConnectorId() + "/" + dirPath + "/%.pb";

            // Find node by S3 key pattern and name
            io.pipeline.repository.entity.Node nodeEntity = io.pipeline.repository.entity.Node
                .find("driveId = ?1 and s3Key like ?2 and name = ?3", drive.id, s3KeyPattern, fileName)
                .firstResult();

            if (nodeEntity == null) {
                throw new DocumentNotFoundException("No node found at path: " + request.getPath());
            }

            return nodeEntity;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(nodeEntity -> {
            if (request.getIncludePayload()) {
                // Load payload from S3
                return Uni.createFrom().item(() -> {
                    try {
                        io.pipeline.repository.entity.Drive drive = driveService.findByName(request.getDrive());
                        byte[] payloadBytes = s3Service.getObject(drive.bucketName, nodeEntity.s3Key);
                        return new NodeManager.NodeResult(nodeEntity, drive, payloadBytes);
                    } catch (Exception e) {
                        LOG.errorf(e, "Failed to load payload for path %s", request.getPath());
                        throw new RuntimeException("Failed to load payload", e);
                    }
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
            } else {
                io.pipeline.repository.entity.Drive drive = driveService.findByName(request.getDrive());
                return Uni.createFrom().item(new NodeManager.NodeResult(nodeEntity, drive, null));
            }
        })
        .map(nodeResult -> {
            // Convert to gRPC Node
            io.pipeline.repository.filesystem.Node.Builder nodeBuilder =
                io.pipeline.repository.filesystem.Node.newBuilder()
                    .setId(nodeResult.node.id)
                    .setDocumentId(nodeResult.node.documentId)
                    .setDriveId(nodeResult.node.driveId)
                    .setName(nodeResult.node.name)
                    .setNodeTypeId(nodeResult.node.nodeTypeId)
                    .setParentId(nodeResult.node.parentId != null ? nodeResult.node.parentId : 0L)
                    .setPath(nodeResult.node.path)
                    .setContentType(nodeResult.node.contentType)
                    .setSizeBytes(nodeResult.node.size != null ? nodeResult.node.size : 0L)
                    .setS3Key(nodeResult.node.s3Key != null ? nodeResult.node.s3Key : "")
                    .setCreatedAt(convertTimestamp(nodeResult.node.createdAt))
                    .setUpdatedAt(convertTimestamp(nodeResult.node.updatedAt))
                    .setMetadata(nodeResult.node.metadata != null ? nodeResult.node.metadata : "")
                    .setType(convertNodeType(nodeResult.node.nodeTypeId))
                    .setServiceType(nodeResult.node.serviceType != null ? nodeResult.node.serviceType : "")
                    .setPayloadType(nodeResult.node.payloadType != null ? nodeResult.node.payloadType : "");

            // Add payload if requested and available
            if (request.getIncludePayload() && nodeResult.payload != null) {
                try {
                    com.google.protobuf.Any payload = com.google.protobuf.Any.parseFrom(nodeResult.payload);
                    nodeBuilder.setPayload(payload);
                } catch (Exception e) {
                    LOG.errorf(e, "Failed to parse payload for path %s", request.getPath());
                    throw new RuntimeException("Failed to parse payload", e);
                }
            }

            return nodeBuilder.build();
        });
    }

    @Override
    public Uni<io.pipeline.repository.filesystem.Node> createNode(CreateNodeRequest request) {
        LOG.infof("Creating node: name=%s, drive=%s, connector=%s",
            request.getName(), request.getDrive(), request.getConnectorId());

        // Validate request
        if (request.getDrive() == null || request.getDrive().isEmpty()) {
            return Uni.createFrom().failure(new InvalidRequestException("INVALID_REQUEST", "Drive is required"));
        }
        if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
            return Uni.createFrom().failure(new InvalidRequestException("INVALID_REQUEST", "Connector ID is required"));
        }
        if (!pathManager.isValidConnectorId(request.getConnectorId())) {
            return Uni.createFrom().failure(new InvalidRequestException("INVALID_REQUEST", "Invalid connector ID format"));
        }

        // Look up drive to get bucket name
        return Uni.createFrom().item(() -> {
            io.pipeline.repository.entity.Drive drive = driveService.findByName(request.getDrive());
            if (drive == null) {
                throw new DriveNotFoundException(request.getDrive());
            }
            return drive;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(drive -> {
            // Extract protobuf metadata
            String protobufType = request.getPayload().getTypeUrl();
            String fileName = request.getName();
            String path = request.getPath() != null && !request.getPath().isEmpty() ?
                         request.getPath() : "";

            // Build S3 metadata
            Map<String, String> s3Metadata = new HashMap<>();
            s3Metadata.put("filename", fileName);
            s3Metadata.put("proto-type", protobufType);
            s3Metadata.put("connector-id", request.getConnectorId());
            s3Metadata.put("upload-type", "direct");
            if (request.getMetadata() != null && !request.getMetadata().isEmpty()) {
                s3Metadata.put("user-metadata", request.getMetadata());
            }

            // Upload protobuf using UploadManager (handles size automatically)
            return uploadManager.uploadProtobuf(
                drive.bucketName, request.getConnectorId(), path, fileName,
                request.getPayload(), s3Metadata
            )
            .flatMap(uploadResult -> {
                // Create node metadata using NodeManager with content hash
                s3Metadata.put("content-hash", uploadResult.contentHash);

                return nodeManager.createNodeMetadata(
                    uploadResult.nodeId, request.getDrive(), request.getConnectorId(),
                    path, fileName, request.getContentType(), uploadResult.size,
                    uploadResult.s3Key, uploadResult.versionId, protobufType, s3Metadata,
                    uploadResult.contentHash
                );
            })
            .map(node -> {
                // Convert to gRPC Node response
                return convertNodeEntityToGrpcNode(node, request.getIncludePayload());
            });
        })
        .onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error in createNode");
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal server error").withCause(e));
            }
        });
    }

    private String buildPathFromParentId(long parentId) {
        if (parentId <= 0) {
            return "";
        }

        // Look up parent node and build path
        io.pipeline.repository.entity.Node parentNode = io.pipeline.repository.entity.Node.findById(parentId);
        if (parentNode != null) {
            return parentNode.path;
        }

        return "";
    }
    
    private io.pipeline.repository.filesystem.Node convertNodeEntityToGrpcNode(
            io.pipeline.repository.entity.Node nodeEntity, boolean includePayload) {
        try {
            io.pipeline.repository.filesystem.Node.Builder nodeBuilder = 
                io.pipeline.repository.filesystem.Node.newBuilder()
                    .setDocumentId(nodeEntity.documentId)
                    .setName(nodeEntity.name)
                    .setPath(nodeEntity.path)
                    .setContentType(nodeEntity.contentType)
                    .setSizeBytes(nodeEntity.size)
                    .setCreatedAt(convertTimestamp(nodeEntity.createdAt))
                    .setUpdatedAt(convertTimestamp(nodeEntity.updatedAt))
                    .setParentId(nodeEntity.parentId != null ? nodeEntity.parentId : 0L)
                    ;
            
            // Add payload if requested
            if (includePayload) {
                try {
                    // Get drive to access bucket name
                    io.pipeline.repository.entity.Drive drive = io.pipeline.repository.entity.Drive.findById(nodeEntity.driveId);
                    if (drive == null) {
                        throw new RepoServiceException("createNode", "DRIVE_NOT_FOUND", 
                            "Drive not found: " + nodeEntity.driveId);
                    }
                    
                    // Retrieve payload from S3
                    byte[] payloadBytes = s3Service.retrieveProtobuf(
                        drive.bucketName, 
                        nodeEntity.s3Key
                    );
                    
                    // Parse the stored Any directly - it's already a serialized protobuf Any
                    com.google.protobuf.Any payload = com.google.protobuf.Any.parseFrom(payloadBytes);
                    nodeBuilder.setPayload(payload);
                    
                    LOG.infof("Hydrated payload for node: documentId=%s, size=%d bytes", 
                        nodeEntity.documentId, payloadBytes.length);
                } catch (Exception e) {
                    LOG.errorf(e, "Failed to hydrate payload for node %s: %s", 
                        nodeEntity.documentId, e.getMessage());
                    // For payload hydration failures, we should fail the request
                    // as the client explicitly requested the payload
                    throw new RepoServiceException("createNode", "PAYLOAD_HYDRATION_FAILED", 
                        "Failed to retrieve payload from S3: " + e.getMessage(), e);
                }
            }
            
            return nodeBuilder.build();
            
        } catch (Exception e) {
            LOG.errorf(e, "Failed to convert node entity to gRPC node: %s", e.getMessage());
            throw new RepoServiceException("createNode", "CONVERSION_FAILED", 
                "Failed to convert node entity: " + e.getMessage(), e);
        }
    }
    
    private io.pipeline.repository.filesystem.Node convertToProtobufNode(
            io.pipeline.repository.entity.Node nodeEntity, 
            com.google.protobuf.Any payload) {
        
        return io.pipeline.repository.filesystem.Node.newBuilder()
            .setId(nodeEntity.id)
            .setDocumentId(nodeEntity.documentId)
            .setDriveId(nodeEntity.driveId)
            .setName(nodeEntity.name)
            .setNodeTypeId(nodeEntity.nodeTypeId)
            .setParentId(nodeEntity.parentId != null ? nodeEntity.parentId : 0L)
            .setPath(nodeEntity.path)
            .setContentType(nodeEntity.contentType)
            .setSizeBytes(nodeEntity.size)
            .setS3Key(nodeEntity.s3Key)
            .setCreatedAt(Timestamp.newBuilder()
                .setSeconds(nodeEntity.createdAt.toEpochSecond())
                .setNanos(nodeEntity.createdAt.getNano())
                .build())
            .setUpdatedAt(Timestamp.newBuilder()
                .setSeconds(nodeEntity.updatedAt.toEpochSecond())
                .setNanos(nodeEntity.updatedAt.getNano())
                .build())
            .setMetadata(nodeEntity.metadata)
            .setPayload(payload)
            .setType(io.pipeline.repository.filesystem.Node.NodeType.FILE) // Default for now
            .build();
    }

    // Helper methods
    
    private io.pipeline.repository.entity.Drive findDriveOrThrow(String driveName) {
        io.pipeline.repository.entity.Drive drive = driveService.findByName(driveName);
        if (drive == null) {
            throw new DriveNotFoundException(driveName);
        }
        return drive;
    }
    
    private io.pipeline.repository.entity.Node findNodeByDocumentIdOrThrow(long driveId, String documentId) {
        io.pipeline.repository.entity.Node node = io.pipeline.repository.entity.Node.findByDocumentId(documentId);
        if (node == null) {
            throw new DocumentNotFoundException(documentId);
        }
        // Verify the node belongs to the correct drive
        if (!node.driveId.equals(driveId)) {
            throw new DocumentNotFoundException(documentId);
        }
        return node;
    }
    
    private Timestamp convertTimestamp(java.time.OffsetDateTime offsetDateTime) {
        if (offsetDateTime == null) {
            return Timestamp.getDefaultInstance();
        }
        java.time.Instant instant = offsetDateTime.toInstant();
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
    
    private io.pipeline.repository.filesystem.Node.NodeType convertNodeType(long nodeTypeId) {
        try {
            NodeType nodeType =
                NodeType.findById(nodeTypeId);
            if (nodeType == null) {
                LOG.warnf("NodeType not found for ID %d, defaulting to FILE", nodeTypeId);
                return io.pipeline.repository.filesystem.Node.NodeType.FILE;
            }
            
            // Map database node type to protobuf node type
            return switch (nodeType.code.toUpperCase()) {
                case "FOLDER" -> io.pipeline.repository.filesystem.Node.NodeType.FOLDER;
                default -> io.pipeline.repository.filesystem.Node.NodeType.FILE;
            };
        } catch (Exception e) {
            LOG.warnf(e, "Failed to lookup NodeType for ID %d, defaulting to FILE", nodeTypeId);
            return io.pipeline.repository.filesystem.Node.NodeType.FILE;
        }
    }

    @Override
    @Transactional
    public Uni<DeleteNodeResponse> deleteNode(DeleteNodeRequest request) {
        return Uni.createFrom().item(Unchecked.supplier(() -> {
            LOG.infof("Deleting node: documentId=%s, drive=%s, recursive=%s",
                request.getDocumentId(), request.getDrive(), request.getRecursive());

            // Look up drive
            io.pipeline.repository.entity.Drive drive = findDriveOrThrow(request.getDrive());

            // Find node by document_id
            io.pipeline.repository.entity.Node nodeEntity = findNodeByDocumentIdOrThrow(drive.id, request.getDocumentId());

            int deletedCount = 0;

            // If recursive and it's a folder, delete children first
            if (request.getRecursive() && nodeEntity.nodeTypeId == 1) { // 1 = FOLDER type
                deletedCount += deleteChildrenRecursively(drive, nodeEntity);
            }

            // Delete from S3 if it has an S3 key (files have S3 keys, folders typically don't)
            if (nodeEntity.s3Key != null && !nodeEntity.s3Key.isEmpty()) {
                try {
                    s3Service.deleteObject(drive.bucketName, nodeEntity.s3Key);
                    LOG.infof("Deleted S3 object: bucket=%s, key=%s", drive.bucketName, nodeEntity.s3Key);
                } catch (Exception e) {
                    LOG.errorf(e, "Failed to delete S3 object for node %s: %s",
                        nodeEntity.documentId, e.getMessage());
                    // Continue with database deletion even if S3 deletion fails
                    // This prevents orphaned database records
                }
            }

            // Delete from database
            nodeEntity.delete();
            deletedCount++;

            LOG.infof("Successfully deleted node: documentId=%s, total deleted=%d",
                request.getDocumentId(), deletedCount);

            return DeleteNodeResponse.newBuilder()
                .setSuccess(true)
                .setDeletedCount(deletedCount)
                .build();

        })).onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error deleting node: documentId=%s", request.getDocumentId());
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal error deleting node").withCause(e));
            }
        });
    }

    private int deleteChildrenRecursively(io.pipeline.repository.entity.Drive drive, io.pipeline.repository.entity.Node parentNode) {
        int deletedCount = 0;

        // Find all children of this node
        List<io.pipeline.repository.entity.Node> children =
            io.pipeline.repository.entity.Node.list("parentId", parentNode.id);

        for (io.pipeline.repository.entity.Node child : children) {
            // Recursively delete children if this child is also a folder
            if (child.nodeTypeId == 1) { // 1 = FOLDER type
                deletedCount += deleteChildrenRecursively(drive, child);
            }

            // Delete S3 object if present
            if (child.s3Key != null && !child.s3Key.isEmpty()) {
                try {
                    s3Service.deleteObject(drive.bucketName, child.s3Key);
                    LOG.debugf("Deleted S3 object for child: bucket=%s, key=%s", drive.bucketName, child.s3Key);
                } catch (Exception e) {
                    LOG.warnf(e, "Failed to delete S3 object for child node %s", child.documentId);
                }
            }

            // Delete the child from database
            child.delete();
            deletedCount++;
        }

        return deletedCount;
    }

    @Override
    @Transactional
    public Uni<io.pipeline.repository.filesystem.Node> updateNode(UpdateNodeRequest request) {
        return Uni.createFrom().item(Unchecked.supplier(() -> {
            LOG.infof("Updating node: documentId=%s, drive=%s",
                request.getDocumentId(), request.getDrive());

            // Look up drive
            io.pipeline.repository.entity.Drive drive = findDriveOrThrow(request.getDrive());

            // Find node by document_id
            io.pipeline.repository.entity.Node nodeEntity = findNodeByDocumentIdOrThrow(drive.id, request.getDocumentId());

            // Update metadata fields if provided
            boolean updated = false;

            if (!request.getName().isEmpty() && !request.getName().equals(nodeEntity.name)) {
                nodeEntity.name = request.getName();
                updated = true;
            }

            if (!request.getContentType().isEmpty() && !request.getContentType().equals(nodeEntity.contentType)) {
                nodeEntity.contentType = request.getContentType();
                updated = true;
            }

            if (!request.getMetadata().isEmpty()) {
                nodeEntity.metadata = request.getMetadata();
                updated = true;
            }

            if (!request.getServiceType().isEmpty()) {
                nodeEntity.serviceType = request.getServiceType();
                updated = true;
            }

            if (!request.getPayloadType().isEmpty()) {
                nodeEntity.payloadType = request.getPayloadType();
                updated = true;
            }

            // Update payload if provided (creates new S3 version)
            if (request.hasPayload() && !request.getPayload().equals(com.google.protobuf.Any.getDefaultInstance())) {
                try {
                    // Store new payload version in S3
                    byte[] payloadBytes = request.getPayload().toByteArray();

                    // Calculate content hash
                    String contentHash = calculateContentHash(payloadBytes);

                    // Get connector ID from existing S3 key if available
                    String connectorId = "default";
                    if (nodeEntity.s3Key != null && nodeEntity.s3Key.contains("/")) {
                        // Extract connector from path like "connectors/filesystem/doc-id.pb"
                        String[] parts = nodeEntity.s3Key.split("/");
                        if (parts.length >= 2 && parts[0].equals("connectors")) {
                            connectorId = parts[1];
                        }
                    }

                    // Update S3 object (will create new version)
                    Map<String, String> s3Metadata = new HashMap<>();
                    s3Metadata.put("filename", nodeEntity.name);
                    s3Metadata.put("content-type", nodeEntity.contentType);
                    s3Metadata.put("proto-type", request.getPayloadType());
                    s3Metadata.put("content-hash", contentHash);

                    S3Service.S3ObjectMetadata s3Result = s3Service.putObject(
                        drive.bucketName,
                        nodeEntity.s3Key,
                        payloadBytes,
                        nodeEntity.contentType,
                        s3Metadata
                    );

                    // Update entity with new version info
                    nodeEntity.s3VersionId = s3Result.versionId;
                    nodeEntity.contentHash = contentHash;
                    nodeEntity.size = (long) payloadBytes.length;
                    updated = true;

                    LOG.infof("Updated payload for node: documentId=%s, new version=%s, size=%d",
                        nodeEntity.documentId, s3Result.versionId, payloadBytes.length);

                } catch (Exception e) {
                    LOG.errorf(e, "Failed to update payload for node %s: %s",
                        nodeEntity.documentId, e.getMessage());
                    throw new RepoServiceException("updateNode", "S3_UPDATE_FAILED",
                        "Failed to update payload in S3: " + e.getMessage(), e);
                }
            }

            if (updated) {
                // Update timestamp
                nodeEntity.updatedAt = java.time.OffsetDateTime.now();

                // Persist changes
                nodeEntity.persist();

                LOG.infof("Successfully updated node: documentId=%s", nodeEntity.documentId);
            } else {
                LOG.infof("No changes to update for node: documentId=%s", nodeEntity.documentId);
            }

            // Build response - never include payload in update response
            return io.pipeline.repository.filesystem.Node.newBuilder()
                .setId(nodeEntity.id)
                .setDocumentId(nodeEntity.documentId)
                .setDriveId(nodeEntity.driveId)
                .setName(nodeEntity.name)
                .setNodeTypeId(nodeEntity.nodeTypeId)
                .setParentId(nodeEntity.parentId != null ? nodeEntity.parentId : 0L)
                .setPath(nodeEntity.path)
                .setContentType(nodeEntity.contentType)
                .setSizeBytes(nodeEntity.size != null ? nodeEntity.size : 0L)
                .setS3Key(nodeEntity.s3Key != null ? nodeEntity.s3Key : "")
                .setCreatedAt(convertTimestamp(nodeEntity.createdAt))
                .setUpdatedAt(convertTimestamp(nodeEntity.updatedAt))
                .setMetadata(nodeEntity.metadata != null ? nodeEntity.metadata : "")
                .setType(convertNodeType(nodeEntity.nodeTypeId))
                .setServiceType(nodeEntity.serviceType != null ? nodeEntity.serviceType : "")
                .setPayloadType(nodeEntity.payloadType != null ? nodeEntity.payloadType : "")
                .build();

        })).onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error updating node: documentId=%s", request.getDocumentId());
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal error updating node").withCause(e));
            }
        });
    }

    @Override
    public Uni<GetChildrenResponse> getChildren(GetChildrenRequest request) {
        return Uni.createFrom().item(() -> request)
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .map(req -> {
            LOG.infof("Getting children: drive=%s, parentId=%d, pageSize=%d",
                request.getDrive(), request.getParentId(), request.getPageSize());

            // Look up drive
            io.pipeline.repository.entity.Drive drive = findDriveOrThrow(request.getDrive());

            // Build query
            String query = "driveId = ?1 and parentId = ?2";
            Object[] params = new Object[] { drive.id, request.getParentId() == 0 ? null : request.getParentId() };

            // Add ordering
            String orderBy = request.getOrderBy().isEmpty() ? "name" : request.getOrderBy();
            String sortOrder = request.getAscending() ? "asc" : "desc";
            query += " order by " + orderBy + " " + sortOrder;

            // Get total count
            long totalCount = io.pipeline.repository.entity.Node.count(
                "driveId = ?1 and parentId = ?2", drive.id, request.getParentId() == 0 ? null : request.getParentId());

            // Get paginated results
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 100;
            int page = 0; // TODO: Implement page token parsing

            List<io.pipeline.repository.entity.Node> nodes =
                io.pipeline.repository.entity.Node.find(query, params)
                    .page(page, pageSize)
                    .list();

            // Convert to protobuf nodes
            GetChildrenResponse.Builder responseBuilder = GetChildrenResponse.newBuilder()
                .setTotalCount((int) totalCount);

            for (io.pipeline.repository.entity.Node node : nodes) {
                responseBuilder.addNodes(convertEntityToProtoNode(node, false));
            }

            // TODO: Implement next page token
            if ((page + 1) * pageSize < totalCount) {
                responseBuilder.setNextPageToken(String.valueOf(page + 1));
            }

            return responseBuilder.build();
        }).onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error getting children: parentId=%d", request.getParentId());
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal error getting children").withCause(e));
            }
        });
    }

    @Override
    public Uni<SearchNodesResponse> searchNodes(SearchNodesRequest request) {
        return Uni.createFrom().item(() -> request)
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .map(req -> {
            LOG.infof("Searching nodes: drive=%s, query=%s, pageSize=%d",
                request.getDrive(), request.getQuery(), request.getPageSize());

            // Look up drive
            io.pipeline.repository.entity.Drive drive = findDriveOrThrow(request.getDrive());

            // Build search query
            StringBuilder queryBuilder = new StringBuilder("driveId = ?1");
            List<Object> params = new java.util.ArrayList<>();
            params.add(drive.id);

            // Add text search on name if query provided
            if (!request.getQuery().isEmpty()) {
                queryBuilder.append(" and lower(name) like ?2");
                params.add("%" + request.getQuery().toLowerCase() + "%");
            }

            // Add path filters
            if (request.getPathsCount() > 0) {
                queryBuilder.append(" and (");
                for (int i = 0; i < request.getPathsCount(); i++) {
                    if (i > 0) queryBuilder.append(" or ");
                    queryBuilder.append("path like ?").append(params.size() + 1);
                    params.add(request.getPaths(i) + "%");
                }
                queryBuilder.append(")");
            }

            // Add metadata filters
            if (request.getMetadataFiltersCount() > 0) {
                for (Map.Entry<String, String> filter : request.getMetadataFiltersMap().entrySet()) {
                    queryBuilder.append(" and metadata like ?").append(params.size() + 1);
                    params.add("%\"" + filter.getKey() + "\":\"" + filter.getValue() + "\"%");
                }
            }

            // Get total count
            long totalCount = io.pipeline.repository.entity.Node.count(
                queryBuilder.toString(), params.toArray());

            // Add sorting
            if (request.getSortByCount() > 0) {
                queryBuilder.append(" order by ");
                for (int i = 0; i < request.getSortByCount(); i++) {
                    if (i > 0) queryBuilder.append(", ");
                    SortField sortField = request.getSortBy(i);
                    queryBuilder.append(sortField.getField())
                        .append(sortField.getAscending() ? " asc" : " desc");
                }
            } else {
                queryBuilder.append(" order by name asc");
            }

            // Get paginated results
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 100;
            int page = 0; // TODO: Implement page token parsing

            List<io.pipeline.repository.entity.Node> nodes =
                io.pipeline.repository.entity.Node.find(queryBuilder.toString(), params.toArray())
                    .page(page, pageSize)
                    .list();

            // Build response
            SearchNodesResponse.Builder responseBuilder = SearchNodesResponse.newBuilder()
                .setTotalCount((int) totalCount);

            for (io.pipeline.repository.entity.Node node : nodes) {
                SearchResult.Builder resultBuilder = SearchResult.newBuilder()
                    .setNode(convertEntityToProtoNode(node, false))
                    .setScore(1.0); // Simple scoring for now

                // Add highlight if query matches name
                if (!request.getQuery().isEmpty() &&
                    node.name.toLowerCase().contains(request.getQuery().toLowerCase())) {
                    resultBuilder.putHighlights("name", node.name);
                }

                responseBuilder.addNodes(resultBuilder.build());
            }

            // TODO: Implement next page token
            if ((page + 1) * pageSize < totalCount) {
                responseBuilder.setNextPageToken(String.valueOf(page + 1));
            }

            responseBuilder.setTookMillis(System.currentTimeMillis()); // Simple timing

            return responseBuilder.build();
        }).onFailure().transform(e -> {
            if (e instanceof RepoServiceException) {
                LOG.errorf("Repository operation failed: %s", ((RepoServiceException) e).getStructuredMessage());
                return new StatusRuntimeException(
                    Status.INVALID_ARGUMENT.withDescription(((RepoServiceException) e).getStructuredMessage()).withCause(e));
            } else {
                LOG.errorf(e, "Unexpected error searching nodes: query=%s", request.getQuery());
                return new StatusRuntimeException(
                    Status.INTERNAL.withDescription("Internal error searching nodes").withCause(e));
            }
        });
    }

    private io.pipeline.repository.filesystem.Node convertEntityToProtoNode(
            io.pipeline.repository.entity.Node nodeEntity, boolean includePayload) {

        io.pipeline.repository.filesystem.Node.Builder nodeBuilder =
            io.pipeline.repository.filesystem.Node.newBuilder()
                .setId(nodeEntity.id)
                .setDocumentId(nodeEntity.documentId)
                .setDriveId(nodeEntity.driveId)
                .setName(nodeEntity.name)
                .setNodeTypeId(nodeEntity.nodeTypeId)
                .setParentId(nodeEntity.parentId != null ? nodeEntity.parentId : 0L)
                .setPath(nodeEntity.path)
                .setContentType(nodeEntity.contentType)
                .setSizeBytes(nodeEntity.size != null ? nodeEntity.size : 0L)
                .setS3Key(nodeEntity.s3Key != null ? nodeEntity.s3Key : "")
                .setCreatedAt(convertTimestamp(nodeEntity.createdAt))
                .setUpdatedAt(convertTimestamp(nodeEntity.updatedAt))
                .setMetadata(nodeEntity.metadata != null ? nodeEntity.metadata : "")
                .setType(convertNodeType(nodeEntity.nodeTypeId))
                .setServiceType(nodeEntity.serviceType != null ? nodeEntity.serviceType : "")
                .setPayloadType(nodeEntity.payloadType != null ? nodeEntity.payloadType : "");

        // Only hydrate payload if explicitly requested
        if (includePayload && nodeEntity.s3Key != null && !nodeEntity.s3Key.isEmpty()) {
            try {
                io.pipeline.repository.entity.Drive drive =
                    io.pipeline.repository.entity.Drive.findById(nodeEntity.driveId);
                if (drive != null) {
                    byte[] payloadBytes = s3Service.retrieveProtobuf(drive.bucketName, nodeEntity.s3Key);
                    com.google.protobuf.Any payload = com.google.protobuf.Any.parseFrom(payloadBytes);
                    nodeBuilder.setPayload(payload);
                }
            } catch (Exception e) {
                LOG.warnf(e, "Failed to hydrate payload for node %s", nodeEntity.documentId);
            }
        }

        return nodeBuilder.build();
    }

    private String calculateContentHash(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(content);
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            LOG.errorf(e, "Failed to initialize SHA-256 digest");
            return "";
        }
    }

    // Drive management operations
    @Override
    public Uni<io.pipeline.repository.filesystem.Drive> createDrive(CreateDriveRequest request) {
        LOG.infof("Creating drive: name=%s, bucket=%s, account=%s",
            request.getName(), request.getBucketName(), request.getAccountId());

        return driveManager.createDrive(
            request.getName(),
            request.getBucketName(),
            request.getAccountId(),
            request.getRegion(),
            request.getDescription(),
            request.getCreateBucket()
        ).map(drive -> io.pipeline.repository.filesystem.Drive.newBuilder()
            .setId(drive.id)
            .setName(drive.name)
            .setBucketName(drive.bucketName)
            .setAccountId(drive.accountId)
            .setRegion(drive.region != null ? drive.region : "")
            .setCredentialsRef(drive.credentialsRef != null ? drive.credentialsRef : "")
            .setStatusId(drive.statusId)
            .setDescription(drive.description != null ? drive.description : "")
            .setCreatedAt(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(drive.createdAt.toEpochSecond())
                .setNanos(drive.createdAt.getNano())
                .build())
            .setMetadata(drive.metadata != null ? drive.metadata : "{}")
            .build());
    }

    @Override
    public Uni<io.pipeline.repository.filesystem.Drive> getDrive(GetDriveRequest request) {
        LOG.infof("Getting drive: name=%s", request.getName());

        return driveManager.getDrive(request.getName())
            .map(drive -> io.pipeline.repository.filesystem.Drive.newBuilder()
                .setId(drive.id)
                .setName(drive.name)
                .setBucketName(drive.bucketName)
                .setAccountId(drive.accountId)
                .setRegion(drive.region != null ? drive.region : "")
                .setCredentialsRef(drive.credentialsRef != null ? drive.credentialsRef : "")
                .setStatusId(drive.statusId)
                .setDescription(drive.description != null ? drive.description : "")
                .setCreatedAt(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(drive.createdAt.toEpochSecond())
                    .setNanos(drive.createdAt.getNano())
                    .build())
                .setMetadata(drive.metadata != null ? drive.metadata : "{}")
                .build());
    }

    // All other methods use default UNIMPLEMENTED from base class
}