package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing metadata key-value pairs for a document.
 */
@Entity
@Table(name = "document_metadata")
public class DocumentMetadata extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @ManyToOne
    @JoinColumn(name = "document_id", nullable = false)
    public Document document;

    @Column(name = "metadata_key", nullable = false)
    public String metadataKey;

    @Column(name = "metadata_value", columnDefinition = "TEXT")
    public String metadataValue;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}
