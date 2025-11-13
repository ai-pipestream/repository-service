package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a node (document) in the repository.
 * Nodes track the upload lifecycle and reference the final Document entity.
 * 
 * Design reference: docs/new-design/00-overview.md
 */
@Entity
@Table(name = "nodes")
public class Node extends PanacheEntity {

    @Column(nullable = false, unique = true)
    public String nodeId;  // UUID for the document

    @ManyToOne
    @JoinColumn(name = "drive_id")
    public Drive drive;

    @ManyToOne
    @JoinColumn(name = "document_id")
    public Document document;  // Link to Document once upload completes

    @Column(nullable = false)
    public String name;

    @Column
    public String contentType;

    @Column
    public Long sizeBytes;

    @Column
    public String s3Key;

    @Column
    public String s3Etag;

    @Column
    public String sha256Hash;

    @Column(nullable = false)
    public String status;  // PENDING, UPLOADING, ACTIVE, FAILED

    @Column(columnDefinition = "JSON")
    public String metadata;

    @Column(nullable = false)
    public Instant createdAt;

    @Column(nullable = false)
    public Instant updatedAt;
}
