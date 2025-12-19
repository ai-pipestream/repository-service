-- Drives, nodes, and PipeDoc records for Phase 1+.

CREATE TABLE drives (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    drive_id VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    description TEXT,
    s3_bucket VARCHAR(255) NOT NULL,
    s3_prefix VARCHAR(1024),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_drive_id (drive_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE nodes (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(255) NOT NULL UNIQUE,
    drive_id BIGINT,
    document_id BIGINT,
    name VARCHAR(1024) NOT NULL,
    content_type VARCHAR(255),
    size_bytes BIGINT,
    s3_key VARCHAR(1024),
    s3_etag VARCHAR(255),
    sha256_hash VARCHAR(128),
    status VARCHAR(64) NOT NULL,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_nodes_drive FOREIGN KEY (drive_id) REFERENCES drives(id),
    CONSTRAINT fk_nodes_document FOREIGN KEY (document_id) REFERENCES documents(id),
    INDEX idx_node_id (node_id),
    INDEX idx_nodes_status (status),
    INDEX idx_nodes_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE pipedocs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(255) NOT NULL UNIQUE,
    checksum VARCHAR(128) NOT NULL,
    drive_name VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    version_id VARCHAR(255),
    etag VARCHAR(255),
    size_bytes BIGINT NOT NULL,
    content_type VARCHAR(255),
    filename VARCHAR(1024),
    pipedoc_bytes LONGBLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pipedocs_doc_id (doc_id),
    INDEX idx_pipedocs_checksum (checksum),
    INDEX idx_pipedocs_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

