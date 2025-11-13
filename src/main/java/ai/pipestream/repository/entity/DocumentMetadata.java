package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing document metadata.
 */
@Entity
@Table(name = "document_metadata")
public class DocumentMetadata extends PanacheEntity {

    @ManyToOne
    @JoinColumn(name = "document_id", nullable = false)
    public Document document;

    @Column(nullable = false)
    public String metadataKey;

    @Column(columnDefinition = "TEXT")
    public String metadataValue;

    @Column(nullable = false)
    public Instant createdAt;
}
