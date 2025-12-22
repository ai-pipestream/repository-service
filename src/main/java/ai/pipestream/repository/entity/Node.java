package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Entity representing a node (document) in the repository.
 * Nodes track the upload lifecycle and reference the final Document entity.
 *
 * Design reference: docs/new-design/00-overview.md
 */
@Entity
@Table(name = "nodes")
public class Node extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(name = "node_id", nullable = false, unique = true)
    public String nodeId;  // UUID for the document

    @ManyToOne
    @JoinColumn(name = "drive_id")
    public Drive drive;

    @ManyToOne
    @JoinColumn(name = "document_id")
    public Document document;  // Link to Document once upload completes

    @Column(nullable = false)
    public String name;

    @Column(name = "content_type")
    public String contentType;

    @Column(name = "size_bytes")
    public Long sizeBytes;

    @Column(name = "s3_key")
    public String s3Key;

    @Column(name = "s3_etag")
    public String s3Etag;

    @Column(name = "sha256_hash")
    public String sha256Hash;

    @Column(nullable = false)
    public String status;  // PENDING, UPLOADING, ACTIVE, FAILED

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    public String metadata;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;
}
