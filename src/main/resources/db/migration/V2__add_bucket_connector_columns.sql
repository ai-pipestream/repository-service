-- Add bucketName and connectorId columns to documents table
ALTER TABLE documents ADD COLUMN bucket_name VARCHAR(255) NOT NULL;
ALTER TABLE documents ADD COLUMN connector_id VARCHAR(255) NOT NULL;

-- Change version column from INT to VARCHAR to support S3 version IDs
ALTER TABLE documents MODIFY COLUMN version VARCHAR(255) NOT NULL;
