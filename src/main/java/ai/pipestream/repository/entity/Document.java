package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a document in the repository.
 */
@Entity
@Table(name = "documents")
public class Document extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(name = "document_id", nullable = false, unique = true)
    public String documentId;

    @Column(nullable = false)
    public String title;

    @Column(columnDefinition = "TEXT")
    public String content;

    @Column(name = "content_type", nullable = false)
    public String contentType;

    @Column(name = "content_size", nullable = false)
    public Long contentSize;

    @Column(name = "storage_location", nullable = false)
    public String storageLocation;

    @Column(nullable = false)
    public String checksum;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;

    @Column(nullable = false)
    public Integer version;

    @Column(nullable = false)
    public String status;
}
