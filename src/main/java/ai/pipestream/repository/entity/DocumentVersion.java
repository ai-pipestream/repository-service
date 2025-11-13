package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a version of a document.
 */
@Entity
@Table(name = "document_versions")
public class DocumentVersion extends PanacheEntity {

    @ManyToOne
    @JoinColumn(name = "document_id", nullable = false)
    public Document document;

    @Column(nullable = false)
    public Integer versionNumber;

    @Column(nullable = false)
    public String storageLocation;

    @Column(nullable = false)
    public String checksum;

    @Column(nullable = false)
    public Long contentSize;

    @Column(columnDefinition = "TEXT")
    public String changeDescription;

    @Column(nullable = false)
    public String createdBy;

    @Column(nullable = false)
    public Instant createdAt;
}
