-- Drives, nodes, and PipeDoc records for Phase 1+.

CREATE TABLE drives (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drive_id VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    description TEXT,
    s3_bucket VARCHAR(255) NOT NULL,
    s3_prefix VARCHAR(1024),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_drive_id ON drives (drive_id);

CREATE TABLE nodes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_nodes_drive FOREIGN KEY (drive_id) REFERENCES drives(id),
    CONSTRAINT fk_nodes_document FOREIGN KEY (document_id) REFERENCES documents(id)
);
CREATE INDEX idx_node_id ON nodes (node_id);
CREATE INDEX idx_nodes_status ON nodes (status);
CREATE INDEX idx_nodes_created_at ON nodes (created_at);

CREATE TABLE pipedocs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    doc_id VARCHAR(255) NOT NULL UNIQUE,
    account_id VARCHAR(255) NOT NULL,
    datasource_id VARCHAR(255) NOT NULL,
    connector_id VARCHAR(255),
    checksum VARCHAR(128) NOT NULL,
    drive_name VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    pipedoc_object_key VARCHAR(1024) NOT NULL,
    version_id VARCHAR(255),
    etag VARCHAR(255) NOT NULL,
    size_bytes BIGINT NOT NULL,
    content_type VARCHAR(255),
    filename VARCHAR(1024) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_pipedocs_doc_id ON pipedocs (doc_id);
CREATE INDEX idx_pipedocs_account_id ON pipedocs (account_id);
CREATE INDEX idx_pipedocs_datasource_id ON pipedocs (datasource_id);
CREATE INDEX idx_pipedocs_checksum ON pipedocs (checksum);
CREATE INDEX idx_pipedocs_created_at ON pipedocs (created_at);

