-- Add soft delete and version tracking columns to documents table

-- Add deleted_at for soft deletes
ALTER TABLE documents
ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;

-- Add version column (default to 1 for existing records)
ALTER TABLE documents
ADD COLUMN version INTEGER NOT NULL DEFAULT 1;

-- Add index for efficient soft delete queries
CREATE INDEX idx_documents_deleted_at ON documents(deleted_at);

-- Add index for version queries
CREATE INDEX idx_documents_version ON documents(document_id, version);

-- Create view for active (non-deleted) documents
CREATE VIEW active_documents AS
SELECT * FROM documents
WHERE deleted_at IS NULL;

-- Add compound index for efficient path-based queries
-- Note: MySQL has a limit on index key length, so we index only first 255 chars of path
CREATE INDEX idx_documents_path_active ON documents(drive_id, path(255), deleted_at);

-- Add index for getting children efficiently
CREATE INDEX idx_documents_children ON documents(drive_id, parent_id, deleted_at);

-- Update path column to ensure it ends with / for directories
UPDATE documents
SET path = CONCAT(path, '/')
WHERE node_type_id = 1  -- FOLDER type
  AND path NOT LIKE '%/'
  AND path IS NOT NULL;