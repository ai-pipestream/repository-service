package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;

import java.time.Instant;

/**
 * Persisted PipeDoc metadata record.
 *
 * Phase 1: created after raw HTTP upload is committed to S3.
 * Stores S3 reference and metadata for the document.
 * The actual PipeDoc protobuf is stored in S3, not in the database.
 */
@Entity
@Table(name = "pipedocs")
public class PipeDocRecord extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(name = "doc_id", nullable = false, unique = true)
    public String docId;

    @Column(name = "account_id", nullable = false)
    public String accountId;

    @Column(name = "datasource_id", nullable = false)
    public String datasourceId;

    @Column(name = "connector_id")
    public String connectorId;

    @Column(nullable = false)
    public String checksum;

    @Column(name = "drive_name", nullable = false)
    public String driveName;

    @Column(name = "object_key", nullable = false, length = 1024)
    public String objectKey;

    @Column(name = "pipedoc_object_key", nullable = false, length = 1024)
    public String pipedocObjectKey;

    @Column(name = "version_id")
    public String versionId;

    @Column(nullable = false)
    public String etag;

    @Column(name = "size_bytes", nullable = false)
    public Long sizeBytes;

    @Column(name = "content_type")
    public String contentType;

    @Column(nullable = false)
    public String filename;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}

