-- Create main documents table (clean, completed uploads only)
CREATE TABLE documents (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    document_id VARCHAR(255) UNIQUE NOT NULL,
    drive_id BIGINT NOT NULL,
    name VARCHAR(500) NOT NULL,
    node_type_id BIGINT NOT NULL,
    parent_id BIGINT NULL,
    path VARCHAR(2000) NOT NULL,
    content_type VARCHAR(255),
    size_bytes BIGINT,
    s3_key VARCHAR(1000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata JSON,
    FOREIGN KEY (drive_id) REFERENCES drives(id) ON DELETE CASCADE,
    FOREIGN KEY (node_type_id) REFERENCES node_type(id),
    FOREIGN KEY (parent_id) REFERENCES documents(id) ON DELETE CASCADE,
    INDEX idx_documents_drive_name (drive_id, name),
    INDEX idx_documents_document_id (document_id),
    INDEX idx_documents_path (path(255))
);
