package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Node entity representing a document in the filesystem.
 * This is the main entity that stores metadata while payloads are in S3.
 */
@Entity
@Table(name = "documents")
public class Node extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(name = "document_id", unique = true, nullable = false)
    public String documentId;              // UUID for external API
    
    @Column(name = "drive_id", nullable = false)
    public Long driveId;                   // Foreign key to Drive
    
    @Column(name = "name", nullable = false)
    public String name;                     // Document name
    
    @Column(name = "node_type_id", nullable = false)
    public Long nodeTypeId;                 // Foreign key to node_type lookup table
    
    @Column(name = "parent_id")
    public Long parentId;                   // Self-reference for hierarchy
    
    @Column(name = "path", nullable = false)
    public String path;                    // Computed path for efficient queries
    
    @Column(name = "content_type")
    public String contentType;             // MIME type
    
    @Column(name = "size_bytes")
    public Long size;                      // Size in bytes
    
    @Column(name = "s3_key")
    public String s3Key;                   // S3 object key within drive's bucket
    
    @Column(name = "content_hash")
    public String contentHash;              // SHA-256 hash of original content for deduplication
    
    @Column(name = "encrypted_content_hash")
    public String encryptedContentHash;    // SHA-256 hash of encrypted content in S3
    
    @Column(name = "s3_version_id")
    public String s3VersionId;              // S3 version ID for versioning support
    
    @Column(name = "created_at")
    public OffsetDateTime createdAt;
    
    @Column(name = "updated_at")
    public OffsetDateTime updatedAt;
    
    @Column(name = "metadata")
    public String metadata;                 // JSON metadata for unstructured data
    
    @Column(name = "service_type")
    public String serviceType;              // Service interface type (e.g., "PipeStepProcessor", "Parser")
    
    @Column(name = "payload_type")
    public String payloadType;              // Actual payload type (e.g., "ModuleProcessRequest", "ModuleProcessResponse")
    
    // Constructors
    public Node() {}
    
    public Node(String documentId, Long driveId, String name, Long nodeTypeId, String path) {
        this.documentId = documentId;
        this.driveId = driveId;
        this.name = name;
        this.nodeTypeId = nodeTypeId;
        this.path = path;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = OffsetDateTime.now();
    }
    
    // Helper methods
    public static Node findByDocumentId(String documentId) {
        return find("documentId", documentId).firstResult();
    }
    
    public static java.util.List<Node> findByDriveId(Long driveId) {
        return find("driveId", driveId).list();
    }
    
    public static java.util.List<Node> findByDriveIdAndName(Long driveId, String name) {
        return find("driveId = ?1 and name = ?2", driveId, name).list();
    }
    
    public static java.util.List<Node> findByDriveIdAndPath(Long driveId, String path) {
        return find("driveId = ?1 and path = ?2", driveId, path).list();
    }
    
    public static java.util.List<Node> findByParentId(Long parentId) {
        return find("parentId", parentId).list();
    }
    
    public static java.util.List<Node> findByDriveIdAndParentId(Long driveId, Long parentId) {
        if (parentId == null) {
            return find("driveId = ?1 and parentId IS NULL", driveId).list();
        } else {
            return find("driveId = ?1 and parentId = ?2", driveId, parentId).list();
        }
    }
    
    // Update timestamp helper
    public void touch() {
        this.updatedAt = OffsetDateTime.now();
    }
}
