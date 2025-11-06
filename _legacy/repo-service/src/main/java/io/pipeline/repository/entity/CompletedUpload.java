package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Completed uploads table for historical record of uploads.
 */
@Entity
@Table(name = "completed_uploads")
public class CompletedUpload extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(name = "document_id", nullable = false)
    public String documentId;              // Foreign key to documents
    
    @Column(name = "upload_id", nullable = false)
    public String uploadId;                // Original upload identifier
    
    @Column(name = "status_id", nullable = false)
    public Long statusId;                  // Final status (COMPLETED or FAILED)
    
    @Column(name = "total_chunks")
    public Integer totalChunks;            // Final chunk count
    
    @Column(name = "error_message")
    public String errorMessage;            // Error details if failed
    
    @Column(name = "started_at")
    public OffsetDateTime startedAt;
    
    @Column(name = "completed_at")
    public OffsetDateTime completedAt;
    
    @Column(name = "archived_at")
    public OffsetDateTime archivedAt;      // When moved to this table
    
    // Constructors
    public CompletedUpload() {}
    
    public CompletedUpload(String documentId, String uploadId, Long statusId, 
                          Integer totalChunks, String errorMessage, 
                          OffsetDateTime startedAt, OffsetDateTime completedAt) {
        this.documentId = documentId;
        this.uploadId = uploadId;
        this.statusId = statusId;
        this.totalChunks = totalChunks;
        this.errorMessage = errorMessage;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.archivedAt = OffsetDateTime.now();
    }
    
    // Helper methods
    public static java.util.List<CompletedUpload> findByDocumentId(String documentId) {
        return find("documentId", documentId).list();
    }
    
    public static java.util.List<CompletedUpload> findByUploadId(String uploadId) {
        return find("uploadId", uploadId).list();
    }
    
    public static java.util.List<CompletedUpload> findByStatusId(Long statusId) {
        return find("statusId", statusId).list();
    }
    
    // Status helpers
    public boolean isSuccessful() {
        return errorMessage == null || errorMessage.isEmpty();
    }
    
    public boolean isFailed() {
        return errorMessage != null && !errorMessage.isEmpty();
    }
}
