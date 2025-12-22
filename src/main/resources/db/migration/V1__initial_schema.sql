-- Initial schema for Repository Service

CREATE TABLE documents (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    content TEXT,
    content_type VARCHAR(100) NOT NULL,
    content_size BIGINT NOT NULL,
    storage_location VARCHAR(1000) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version INT NOT NULL DEFAULT 1,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE'
);
CREATE INDEX idx_document_id ON documents (document_id);
CREATE INDEX idx_status ON documents (status);
CREATE INDEX idx_created_at ON documents (created_at);

CREATE TABLE document_metadata (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id BIGINT NOT NULL,
    metadata_key VARCHAR(255) NOT NULL,
    metadata_value TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX idx_document_metadata ON document_metadata (document_id, metadata_key);
CREATE INDEX idx_metadata_key ON document_metadata (metadata_key);

CREATE TABLE document_versions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id BIGINT NOT NULL,
    version_number INT NOT NULL,
    storage_location VARCHAR(1000) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    content_size BIGINT NOT NULL,
    change_description TEXT,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    CONSTRAINT unique_document_version UNIQUE (document_id, version_number)
);
CREATE INDEX idx_document_versions ON document_versions (document_id, version_number);
CREATE INDEX idx_versions_created_at ON document_versions (created_at);
