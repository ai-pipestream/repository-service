package io.pipeline.repository.test;

import io.pipeline.repository.entity.*;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Test helper for creating entities with minimal boilerplate.
 * Handles common entity creation patterns for tests.
 */
@ApplicationScoped
public class EntityTestHelper {
    
    private static final Logger LOG = Logger.getLogger(EntityTestHelper.class);
    
    /**
     * Create a test drive with sensible defaults.
     */
    public Drive createTestDrive(String namePrefix, String bucketName) {
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        if (activeStatus == null) {
            throw new IllegalStateException("ACTIVE DriveStatus not found - check import.sql");
        }
        
        Drive drive = new Drive();
        drive.name = namePrefix + "-" + System.currentTimeMillis();
        drive.bucketName = bucketName;
        drive.accountId = "test-customer-" + UUID.randomUUID().toString().substring(0, 8);
        drive.statusId = activeStatus.id;
        drive.description = "Test drive created by EntityTestHelper";
        drive.createdAt = OffsetDateTime.now();
        drive.metadata = "{\"test\": true}";
        drive.persist();
        drive.flush(); // Ensure immediate persistence
        
        LOG.infof("Created test drive: id=%d, name=%s, bucket=%s", 
            drive.id, drive.name, drive.bucketName);
        
        return drive;
    }
    
    /**
     * Create a test node with sensible defaults.
     */
    public Node createTestNode(Drive drive, String namePrefix, String nodeTypeCode) {
        NodeType nodeType = NodeType.findByCode(nodeTypeCode);
        if (nodeType == null) {
            throw new IllegalStateException("NodeType not found: " + nodeTypeCode);
        }
        
        Node node = new Node();
        node.documentId = UUID.randomUUID().toString();
        node.driveId = drive.id;
        node.name = namePrefix + "-" + System.currentTimeMillis() + ".txt";
        node.nodeTypeId = nodeType.id;
        node.parentId = null; // Root level by default
        node.path = "/" + node.name;
        node.contentType = "text/plain";
        node.size = 1024L; // Default size
        node.s3Key = node.documentId + ".pb";
        node.createdAt = OffsetDateTime.now();
        node.updatedAt = OffsetDateTime.now();
        node.metadata = "{\"test\": true}";
        node.persist();
        node.flush(); // Ensure immediate persistence
        
        LOG.infof("Created test node: id=%d, documentId=%s, name=%s", 
            node.id, node.documentId, node.name);
        
        return node;
    }
    
    /**
     * Create a parent-child node hierarchy.
     */
    public NodeHierarchy createNodeHierarchy(Drive drive, String parentName, String childName) {
        NodeType fileType = NodeType.findByCode("FILE");
        
        // Create parent
        Node parent = new Node();
        parent.documentId = UUID.randomUUID().toString();
        parent.driveId = drive.id;
        parent.name = parentName;
        parent.nodeTypeId = fileType.id;
        parent.parentId = null;
        parent.path = "/" + parentName;
        parent.contentType = "application/folder";
        parent.createdAt = OffsetDateTime.now();
        parent.updatedAt = OffsetDateTime.now();
        parent.metadata = "{\"type\": \"folder\"}";
        parent.persist();
        parent.flush();
        
        // Create child
        Node child = new Node();
        child.documentId = UUID.randomUUID().toString();
        child.driveId = drive.id;
        child.name = childName;
        child.nodeTypeId = fileType.id;
        child.parentId = parent.id;
        child.path = "/" + parentName + "/" + childName;
        child.contentType = "text/plain";
        child.createdAt = OffsetDateTime.now();
        child.updatedAt = OffsetDateTime.now();
        child.metadata = "{\"type\": \"file\"}";
        child.persist();
        child.flush();
        
        LOG.infof("Created hierarchy: parent.id=%d, child.id=%d, child.parentId=%d", 
            parent.id, child.id, child.parentId);
        
        return new NodeHierarchy(parent, child);
    }
    
    /**
     * Get lookup entities with validation.
     */
    public LookupEntities getLookupEntities() {
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        DriveStatus inactiveStatus = DriveStatus.findByCode("INACTIVE");
        NodeType fileType = NodeType.findByCode("FILE");
        NodeType pipeDocType = NodeType.findByCode("PIPEDOC");
        UploadStatus pendingStatus = UploadStatus.findByCode("PENDING");
        UploadStatus completedStatus = UploadStatus.findByCode("COMPLETED");
        
        if (activeStatus == null || inactiveStatus == null || fileType == null || 
            pipeDocType == null || pendingStatus == null || completedStatus == null) {
            throw new IllegalStateException("Missing lookup data - check import.sql");
        }
        
        return new LookupEntities(activeStatus, inactiveStatus, fileType, pipeDocType, 
                                 pendingStatus, completedStatus);
    }
    
    /**
     * Result object for node hierarchy creation.
     */
    public static class NodeHierarchy {
        public final Node parent;
        public final Node child;
        
        public NodeHierarchy(Node parent, Node child) {
            this.parent = parent;
            this.child = child;
        }
    }
    
    /**
     * Container for all lookup entities.
     */
    public static class LookupEntities {
        public final DriveStatus activeStatus;
        public final DriveStatus inactiveStatus;
        public final NodeType fileType;
        public final NodeType pipeDocType;
        public final UploadStatus pendingStatus;
        public final UploadStatus completedStatus;
        
        public LookupEntities(DriveStatus activeStatus, DriveStatus inactiveStatus,
                             NodeType fileType, NodeType pipeDocType,
                             UploadStatus pendingStatus, UploadStatus completedStatus) {
            this.activeStatus = activeStatus;
            this.inactiveStatus = inactiveStatus;
            this.fileType = fileType;
            this.pipeDocType = pipeDocType;
            this.pendingStatus = pendingStatus;
            this.completedStatus = completedStatus;
        }
    }
}
