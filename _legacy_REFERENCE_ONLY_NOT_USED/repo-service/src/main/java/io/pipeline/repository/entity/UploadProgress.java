package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Upload progress table for tracking active uploads.
 */
@Entity
@Table(name = "upload_progress")
public class UploadProgress extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(name = "document_id", nullable = false)
    public String documentId;              // Foreign key to documents
    
    @Column(name = "upload_id", unique = true, nullable = false)
    public String uploadId;                // Unique upload identifier
    
    @Column(name = "status_id", nullable = false)
    public Long statusId;                  // Foreign key to upload_status lookup table
    
    @Column(name = "total_chunks")
    public Integer totalChunks;            // For multi-part uploads
    
    @Column(name = "completed_chunks")
    public Integer completedChunks;        // Progress tracking
    
    @Column(name = "error_message")
    public String errorMessage;            // Error details if failed
    
    @Column(name = "started_at")
    public OffsetDateTime startedAt;
    
    @Column(name = "completed_at")
    public OffsetDateTime completedAt;
    
    // Constructors
    public UploadProgress() {}
    
    public UploadProgress(String documentId, String uploadId, Long statusId) {
        this.documentId = documentId;
        this.uploadId = uploadId;
        this.statusId = statusId;
        this.startedAt = OffsetDateTime.now();
        this.completedChunks = 0;
    }
    
    // Helper methods
    public static UploadProgress findByDocumentId(String documentId) {
        return find("documentId", documentId).firstResult();
    }
    
    public static UploadProgress findByUploadId(String uploadId) {
        return find("uploadId", uploadId).firstResult();
    }
    
    public static java.util.List<UploadProgress> findByStatusId(Long statusId) {
        return find("statusId", statusId).list();
    }
    
    // Progress calculation
    public double getProgressPercentage() {
        if (totalChunks == null || totalChunks == 0) {
            return 0.0;
        }
        return (double) completedChunks / totalChunks * 100.0;
    }
    
    // Status helpers
    public boolean isCompleted() {
        return completedAt != null;
    }
    
    public boolean isFailed() {
        return errorMessage != null && !errorMessage.isEmpty();
    }
}
