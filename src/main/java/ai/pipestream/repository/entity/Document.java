package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a document in the repository.
 */
@Entity
@Table(name = "documents")
public class Document extends PanacheEntity {

    @Column(nullable = false, unique = true)
    public String documentId;

    @Column(nullable = false)
    public String title;

    @Column(columnDefinition = "TEXT")
    public String content;

    @Column(nullable = false)
    public String contentType;

    @Column(nullable = false)
    public Long contentSize;

    @Column(nullable = false)
    public String storageLocation;

    @Column(nullable = false)
    public String checksum;

    @Column(nullable = false)
    public Instant createdAt;

    @Column(nullable = false)
    public Instant updatedAt;

    @Column(nullable = false)
    public Integer version;

    @Column(nullable = false)
    public String status;
}
