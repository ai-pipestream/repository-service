-- Add content hash and S3 version ID fields to documents table for deduplication and versioning support
ALTER TABLE documents ADD COLUMN content_hash VARCHAR(255);
ALTER TABLE documents ADD COLUMN s3_version_id VARCHAR(255);

-- Create index on content_hash for efficient deduplication queries
CREATE INDEX idx_documents_content_hash ON documents(content_hash);

-- Create index on s3_version_id for version tracking
CREATE INDEX idx_documents_s3_version_id ON documents(s3_version_id);

-- Add comments on columns (MySQL syntax)
ALTER TABLE documents MODIFY COLUMN content_hash VARCHAR(255) COMMENT 'SHA-256 hash of content for deduplication';
ALTER TABLE documents MODIFY COLUMN s3_version_id VARCHAR(255) COMMENT 'S3 version ID for versioning support';