package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;

/**
 * Entity representing a logical document in the repository.
 * Serves as the "Card Catalog" identity that groups multiple PipeDocRecords (DAG states).
 */
@Entity
@Table(name = "documents")
public class Document extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    /**
     * Logical document identifier (UUID).
     * This ID persists across all versions and processing hops.
     */
    @Column(name = "document_id", nullable = false, unique = true)
    public String documentId;

    @Column(nullable = false)
    public String title;

    @Column(nullable = false)
    public String filename;

    @Column(name = "account_id", nullable = false)
    public String accountId;

    @Column(name = "datasource_id", nullable = false)
    public String datasourceId;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "acls")
    public List<String> acls;

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

    /**
     * Logical status of the document lifecycle (e.g. "INTAKE", "PROCESSING", "COMPLETED").
     */
    @Column(nullable = false)
    public String status;

    @Column(columnDefinition = "TEXT")
    public String content;
}
