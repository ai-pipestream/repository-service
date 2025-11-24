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

    @Column(unique = true, nullable = false)
    public String name; // Customer-facing drive name

    @Column(name = "bucket_name", unique = true, nullable = false)
    public String bucketName; // Actual S3 bucket name

    @Column(name = "account_id", nullable = false)
    public String accountId; // Account identifier for multi-tenancy

    @Column(name = "region")
    public String region; // S3 region (optional)

    @Column(name = "credentials_ref")
    public String credentialsRef; // String reference to external secret management

    @Column(name = "status_id")
    public Long statusId; // Foreign key to drive_status lookup table

    @Column(name = "description")
    public String description; // Drive description

    @Column(name = "created_at")
    public Instant createdAt;

    @Column(name = "metadata", columnDefinition = "TEXT")
    public String metadata; // JSON metadata for unstructured data

    // Constructors
    public Drive() {
    }

    public Drive(String name, String bucketName, String accountId) {
        this.name = name;
        this.bucketName = bucketName;
        this.accountId = accountId;
        this.createdAt = Instant.now();
    }

    // Finder methods (Active Record pattern)
    public static Drive findByName(String name) {
        return find("name", name).firstResult();
    }

    public static Drive findByBucketName(String bucketName) {
        return find("bucketName", bucketName).firstResult();
    }

    public static java.util.List<Drive> findByAccountId(String accountId) {
        return find("accountId", accountId).list();
    }
}
