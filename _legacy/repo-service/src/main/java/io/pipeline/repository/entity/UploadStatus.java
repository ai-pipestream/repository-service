package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

/**
 * Upload status lookup table for tracking upload progress.
 */
@Entity
@Table(name = "upload_status")
public class UploadStatus extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(unique = true, nullable = false)
    public String code;                    // Status code (PENDING, UPLOADING, COMPLETED, FAILED)
    
    @Column(nullable = false)
    public String description;             // Human-readable description
    
    @Column(name = "is_final")
    public Boolean isFinal = false;        // Whether this is a final status
    
    // Constructors
    public UploadStatus() {}
    
    public UploadStatus(String code, String description, Boolean isFinal) {
        this.code = code;
        this.description = description;
        this.isFinal = isFinal;
    }
    
    // Helper methods
    public static UploadStatus findByCode(String code) {
        return find("code", code).firstResult();
    }
    
    public static java.util.List<UploadStatus> findFinalStatuses() {
        return find("isFinal", true).list();
    }
}
