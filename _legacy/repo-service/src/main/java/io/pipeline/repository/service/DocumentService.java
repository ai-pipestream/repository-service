package io.pipeline.repository.service;

import io.pipeline.repository.entity.*;
import io.pipeline.repository.exception.*;
import io.pipeline.repository.services.S3Service;
import io.pipeline.repository.services.EventPublisher;
import io.pipeline.repository.util.HashUtil;
import io.pipeline.repository.util.RequestContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Service layer for document operations.
 * Handles database transactions, S3 storage, and event emission.
 */
@ApplicationScoped
public class DocumentService {
    
    private static final Logger LOG = Logger.getLogger(DocumentService.class);
    
    @Inject
    S3Service s3Service;
    
    @Inject
    EventPublisher eventPublisher;
    
    @Inject
    Drive.DriveService driveService;
    
    /**
     * Create a new document with payload storage.
     */
    @Transactional
    public DocumentResult createDocument(String driveName, String name, String contentType, 
                                       byte[] payload, String payloadType) {
        return createDocumentWithTopic(driveName, name, contentType, payload, payloadType, null, null);
    }
    
    /**
     * Create a new document with connector context.
     */
    @Transactional
    public DocumentResult createDocument(String driveName, String name, String contentType,
                                       byte[] payload, String payloadType, String connectorId) {
        return createDocumentWithTopic(driveName, name, contentType, payload, payloadType, null, connectorId);
    }
    
    /**
     * Create a new document with custom topic (for testing).
     */
    @Transactional
    public DocumentResult createDocumentWithTopic(String driveName, String name, String contentType, 
                                                 byte[] payload, String payloadType, String customTopic) {
        return createDocumentWithTopic(driveName, name, contentType, payload, payloadType, customTopic, null);
    }
    
    /**
     * Create a new document with custom topic and connector context.
     */
    @Transactional
    public DocumentResult createDocumentWithTopic(String driveName, String name, String contentType,
                                                 byte[] payload, String payloadType, String customTopic,
                                                 String connectorId) {
        try {
            // Validation - allow null payload for S3 storage
            validateCreateRequest(driveName, name, payload, true);
            
            // Generate document ID
            String documentId = UUID.randomUUID().toString();
            
            // Look up drive
            Drive drive = findDriveOrThrow(driveName);
            
            // Look up node type (default to FILE for now)
            NodeType nodeType = findNodeTypeOrThrow("FILE");
            
            // Set request context if not already set
            if (RequestContext.getRequestId() == null) {
                RequestContext.setContext(
                    RequestContext.generateRequestId("createDocument", connectorId),
                    "createDocument",
                    connectorId,
                    drive.accountId
                );
            }
            
            LOG.infof("Creating document: documentId=%s, drive=%s, name=%s, size=%d, requestId=%s", 
                documentId, driveName, name, payload != null ? payload.length : 0, 
                RequestContext.getRequestId());
            
            // Calculate content hash if payload exists
            String contentHash = null;
            if (payload != null) {
                contentHash = HashUtil.calculateSHA256(payload);
                LOG.debugf("Calculated content hash for document %s: %s", documentId, contentHash);
            }
            
            // Store in S3 (critical path - must succeed)
            // Skip S3 storage if payload is null (content already in S3 via multipart upload)
            S3Service.S3ObjectMetadata s3Metadata;
            if (payload != null) {
                s3Metadata = storeInS3(drive, documentId, payload, contentType);
            } else {
                // Content already in S3 via multipart upload, create metadata for database
                s3Metadata = new S3Service.S3ObjectMetadata();
                s3Metadata.s3Key = documentId + ".pb";
                s3Metadata.contentType = contentType;
                s3Metadata.size = 0L; // Size will be updated from actual S3 object
                s3Metadata.versionId = null; // Will be retrieved from S3 if needed
            }
            
            // Store in database (critical path - must succeed)
            Node nodeEntity = createNodeEntity(documentId, drive, nodeType, name, contentType, 
                payload != null ? payload.length : 0, s3Metadata.s3Key, contentHash, s3Metadata.versionId);
            
            LOG.infof("Document created successfully: id=%d, documentId=%s, s3Key=%s, versionId=%s", 
                nodeEntity.id, documentId, s3Metadata.s3Key, s3Metadata.versionId);
            
            // Emit events (non-blocking, fire-and-forget)
            emitDocumentCreatedEvent(nodeEntity, drive, s3Metadata, payloadType, contentHash, connectorId, customTopic);
            
            return DocumentResult.success(nodeEntity, s3Metadata);
            
        } catch (RepoServiceException e) {
            LOG.errorf(e, "Document creation failed: %s", e.getStructuredMessage());
            throw e;
        } catch (Exception e) {
            LOG.errorf(e, "Unexpected error during document creation");
            throw new DatabaseOperationException("createDocument", "Node", e);
        }
    }
    
    /**
     * Get a document by ID, optionally including payload.
     */
    @Transactional
    public DocumentResult getDocument(String documentId, boolean includePayload) {
        try {
            LOG.infof("Getting document: documentId=%s, includePayload=%s", documentId, includePayload);
            
            // Find node in database
            Node nodeEntity = Node.findByDocumentId(documentId);
            if (nodeEntity == null) {
                throw new DocumentNotFoundException(documentId);
            }
            
            // Get drive info
            Drive drive = Drive.findById(nodeEntity.driveId);
            if (drive == null) {
                throw new DriveNotFoundException(nodeEntity.driveId);
            }
            
            S3Service.S3ObjectMetadata s3Metadata = null;
            byte[] payload = null;
            
            if (includePayload) {
                // Download from S3 if payload requested
                try {
                    payload = s3Service.retrieveProtobuf(drive.bucketName, documentId);
                    s3Metadata = s3Service.getProtobufMetadata(drive.bucketName, documentId);
                    
                    LOG.infof("Retrieved payload: documentId=%s, size=%d bytes", 
                        documentId, payload.length);
                        
                } catch (Exception e) {
                    throw S3StorageException.downloadFailed(drive.bucketName, nodeEntity.s3Key, e);
                }
            }
            
            // Emit access event (non-blocking)
            emitDocumentAccessedEvent(nodeEntity, drive);
            
            return DocumentResult.success(nodeEntity, s3Metadata, payload);
            
        } catch (RepoServiceException e) {
            LOG.errorf(e, "Document retrieval failed: %s", e.getStructuredMessage());
            throw e;
        } catch (Exception e) {
            LOG.errorf(e, "Unexpected error during document retrieval");
            throw new DatabaseOperationException("getDocument", "Node", e);
        }
    }
    
    private void validateCreateRequest(String driveName, String name, byte[] payload) {
        validateCreateRequest(driveName, name, payload, false);
    }
    
    private void validateCreateRequest(String driveName, String name, byte[] payload, boolean allowNullPayload) {
        if (driveName == null || driveName.trim().isEmpty()) {
            throw InvalidRequestException.missingField("createDocument", "driveName");
        }
        if (name == null || name.trim().isEmpty()) {
            throw InvalidRequestException.missingField("createDocument", "name");
        }
        if (!allowNullPayload && (payload == null || payload.length == 0)) {
            throw InvalidRequestException.emptyPayload("createDocument");
        }
    }
    
    private Drive findDriveOrThrow(String driveName) {
        Drive drive = driveService.findByName(driveName);
        if (drive == null) {
            throw new DriveNotFoundException(driveName);
        }
        return drive;
    }
    
    private NodeType findNodeTypeOrThrow(String code) {
        NodeType nodeType = NodeType.findByCode(code);
        if (nodeType == null) {
            throw new DatabaseOperationException("findNodeType", "NodeType", 
                "NodeType not found: " + code);
        }
        return nodeType;
    }
    
    private S3Service.S3ObjectMetadata storeInS3(Drive drive, String documentId, 
                                                 byte[] payload, String contentType) {
        try {
            return s3Service.storeProtobuf(drive.bucketName, documentId, payload, contentType);
        } catch (Exception e) {
            throw S3StorageException.uploadFailed(drive.bucketName, documentId + ".pb", e);
        }
    }
    
    private Node createNodeEntity(String documentId, Drive drive, NodeType nodeType, 
                                 String name, String contentType, long size, String s3Key,
                                 String contentHash, String s3VersionId) {
        try {
            Node nodeEntity = new Node();
            nodeEntity.documentId = documentId;
            nodeEntity.driveId = drive.id;
            nodeEntity.name = name;
            nodeEntity.nodeTypeId = nodeType.id;
            nodeEntity.parentId = null; // Root level for now
            nodeEntity.path = "/" + name; // Simple path
            nodeEntity.contentType = contentType;
            nodeEntity.size = size;
            nodeEntity.s3Key = s3Key;
            nodeEntity.contentHash = contentHash;
            nodeEntity.s3VersionId = s3VersionId;
            nodeEntity.createdAt = OffsetDateTime.now();
            nodeEntity.updatedAt = OffsetDateTime.now();
            nodeEntity.metadata = "{}"; // Empty JSON
            
            nodeEntity.persist();
            return nodeEntity;
            
        } catch (Exception e) {
            throw DatabaseOperationException.persistFailed("Node", e);
        }
    }
    
    private void emitDocumentCreatedEvent(Node nodeEntity, Drive drive, 
                                        S3Service.S3ObjectMetadata s3Metadata, 
                                        String payloadType, String contentHash, 
                                        String connectorId, String customTopic) {
        try {
            // Extract S3 version ID from metadata
            String s3VersionId = s3Metadata.versionId != null ? s3Metadata.versionId : "";
            
            // Use provided content hash or empty string
            if (contentHash == null) {
                contentHash = "";
            }
            
            // Use provided connector ID or empty string
            if (connectorId == null) {
                connectorId = "";
            }
            
            // Emit event using fire-and-forget with Cancellable for control
            io.smallrye.mutiny.subscription.Cancellable eventCancellable;
            if (customTopic != null) {
                // Use custom topic (for testing)
                eventCancellable = eventPublisher.publishDocumentCreatedToTopic(
                    customTopic, nodeEntity.documentId, drive.name, drive.id, nodeEntity.path,
                    nodeEntity.name, nodeEntity.contentType, nodeEntity.size,
                    s3Metadata.s3Key, payloadType, 
                    contentHash, s3VersionId, drive.accountId, connectorId);
            } else {
                // Use default topic (for production)
                eventCancellable = eventPublisher.publishDocumentCreatedAndForget(
                    nodeEntity.documentId, drive.name, drive.id, nodeEntity.path,
                    nodeEntity.name, nodeEntity.contentType, nodeEntity.size,
                    s3Metadata.s3Key, payloadType,
                    contentHash, s3VersionId, drive.accountId, connectorId);
            }
            
            // Could store cancellable for later cancellation if needed
            LOG.debugf("DocumentCreated event emission started: %s (cancellable: %s)", 
                nodeEntity.documentId, eventCancellable != null);
            
        } catch (Exception e) {
            // Log warning but don't fail the operation
            LOG.warnf("Failed to build DocumentCreated event for %s: %s", 
                nodeEntity.documentId, e.getMessage());
        }
    }
    
    private void emitDocumentAccessedEvent(Node nodeEntity, Drive drive) {
        try {
            // Emit access event for analytics (fire-and-forget)
            // TODO: Implement when we have DocumentAccessed event type
            LOG.debugf("Document accessed: %s", nodeEntity.documentId);
        } catch (Exception e) {
            LOG.debugf("Failed to emit DocumentAccessed event for %s: %s", 
                nodeEntity.documentId, e.getMessage());
        }
    }
    
    /**
     * Result object for document operations.
     */
    public static class DocumentResult {
        public final Node nodeEntity;
        public final S3Service.S3ObjectMetadata s3Metadata;
        public final byte[] payload;
        public final boolean success;
        
        private DocumentResult(Node nodeEntity, S3Service.S3ObjectMetadata s3Metadata, 
                              byte[] payload, boolean success) {
            this.nodeEntity = nodeEntity;
            this.s3Metadata = s3Metadata;
            this.payload = payload;
            this.success = success;
        }
        
        public static DocumentResult success(Node nodeEntity, S3Service.S3ObjectMetadata s3Metadata) {
            return new DocumentResult(nodeEntity, s3Metadata, null, true);
        }
        
        public static DocumentResult success(Node nodeEntity, S3Service.S3ObjectMetadata s3Metadata, 
                                           byte[] payload) {
            return new DocumentResult(nodeEntity, s3Metadata, payload, true);
        }
    }
}
