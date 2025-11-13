package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entity representing a drive (storage location) in the repository.
 * A drive is a logical container for nodes (documents).
 * 
 * Design reference: docs/new-design/00-overview.md
 */
@Entity
@Table(name = "drives")
public class Drive extends PanacheEntity {

    @Column(nullable = false, unique = true)
    public String driveId;

    @Column(nullable = false)
    public String name;

    @Column(columnDefinition = "TEXT")
    public String description;

    @Column(nullable = false)
    public String s3Bucket;

    @Column
    public String s3Prefix;

    @Column(nullable = false)
    public Instant createdAt;

    @Column(nullable = false)
    public Instant updatedAt;
}
