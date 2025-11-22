-- Add encrypted content hash field for storing SHA256 of encrypted S3 content
ALTER TABLE documents ADD COLUMN encrypted_content_hash VARCHAR(255);

-- Create index on encrypted_content_hash for efficient integrity verification
CREATE INDEX idx_documents_encrypted_content_hash ON documents(encrypted_content_hash);

-- Add comment on column
ALTER TABLE documents MODIFY COLUMN encrypted_content_hash VARCHAR(255) COMMENT 'SHA-256 hash of encrypted content stored in S3';
