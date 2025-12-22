package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a version of a document.
 */
@Entity
@Table(name = "document_versions")
public class DocumentVersion extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @ManyToOne
    @JoinColumn(name = "document_id", nullable = false)
    public Document document;

    @Column(name = "version_number", nullable = false)
    public Integer versionNumber;

    @Column(name = "storage_location", nullable = false)
    public String storageLocation;

    @Column(nullable = false)
    public String checksum;

    @Column(name = "content_size", nullable = false)
    public Long contentSize;

    @Column(name = "change_description", columnDefinition = "TEXT")
    public String changeDescription;

    @Column(name = "created_by", nullable = false)
    public String createdBy;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}
