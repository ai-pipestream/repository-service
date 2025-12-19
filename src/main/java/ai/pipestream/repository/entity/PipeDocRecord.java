package ai.pipestream.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Persisted PipeDoc-for-parsing record.
 *
 * Phase 1: created after raw HTTP upload is committed to S3.
 * Stores the serialized PipeDoc protobuf and the S3 reference used for hydration.
 */
@Entity
@Table(name = "pipedocs")
public class PipeDocRecord extends PanacheEntity {

    @Column(nullable = false, unique = true)
    public String docId;

    @Column(nullable = false)
    public String checksum;

    @Column(nullable = false)
    public String driveName;

    @Column(nullable = false, length = 1024)
    public String objectKey;

    @Column
    public String versionId;

    @Column
    public String etag;

    @Column(nullable = false)
    public Long sizeBytes;

    @Column
    public String contentType;

    @Column
    public String filename;

    @Lob
    @Column(nullable = false, columnDefinition = "LONGBLOB")
    public byte[] pipedocBytes;

    @Column(nullable = false)
    public Instant createdAt;
}

