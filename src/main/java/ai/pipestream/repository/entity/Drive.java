package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;

import java.time.Instant;

/**
 * Entity representing a logical drive (container for nodes).
 */
@Entity
@Table(name = "drives")
public class Drive extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(name = "drive_id", nullable = false, unique = true)
    public String driveId;

    @Column(nullable = false)
    public String name;

    @Column(columnDefinition = "TEXT")
    public String description;

    @Column(name = "s3_bucket", nullable = false)
    public String s3Bucket;

    @Column(name = "s3_prefix")
    public String s3Prefix;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;
}
