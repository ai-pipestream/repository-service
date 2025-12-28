package ai.pipestream.repository.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Persisted PipeDoc metadata record.
 * <p>
 * Stores metadata for a document at a specific state (source_node_id) in the pipeline.
 * The actual PipeDoc protobuf is stored in S3, not in the database.
 * <p>
 * Multiple records can exist for the same doc_id (one per source_node_id).
 * The node_id (UUID) is the primary key and can be used as Kafka key for partitioning.
 */
@Entity
@Table(name = "pipedocs")
public class PipeDocRecord extends PanacheEntityBase {

    /**
     * Repository's unique identifier (UUID) - primary key.
     * This UUID can be used as Kafka key for message partitioning and ordering.
     */
    @Id
    @Column(name = "node_id", nullable = false)
    public UUID nodeId;

    /**
     * Document identifier - the logical document ID across all pipeline states.
     * Multiple records can have the same doc_id (one per source_node_id).
     */
    @Column(name = "doc_id", nullable = false)
    public String docId;

    /**
     * Graph location ID where the document was at this state.
     * Can be either a datasource_id (for initial intake) or a graph node ID (after processing).
     * Part of the composite key for DocumentReference lookups (doc_id, graph_address_id, account_id).
     */
    @Column(name = "graph_address_id", nullable = false)
    public String graphAddressId;

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

