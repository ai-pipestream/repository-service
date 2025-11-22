-- Create upload progress table (active uploads)
CREATE TABLE upload_progress (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    document_id VARCHAR(255) NOT NULL,
    upload_id VARCHAR(255) UNIQUE NOT NULL,
    status_id BIGINT NOT NULL,
    total_chunks INT,
    completed_chunks INT,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    FOREIGN KEY (document_id) REFERENCES documents(document_id) ON DELETE CASCADE,
    FOREIGN KEY (status_id) REFERENCES upload_status(id),
    INDEX idx_upload_document (document_id),
    INDEX idx_upload_status (status_id)
);

-- Create completed uploads table (historical record)
CREATE TABLE completed_uploads (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    document_id VARCHAR(255) NOT NULL,
    upload_id VARCHAR(255) NOT NULL,
    status_id BIGINT NOT NULL,
    total_chunks INT,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(document_id) ON DELETE CASCADE,
    FOREIGN KEY (status_id) REFERENCES upload_status(id),
    INDEX idx_completed_document (document_id),
    INDEX idx_completed_status (status_id)
);
