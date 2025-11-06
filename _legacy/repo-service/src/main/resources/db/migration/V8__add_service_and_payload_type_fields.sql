-- Add service_type and payload_type fields to documents table for service interface and payload type tracking
ALTER TABLE documents ADD COLUMN service_type VARCHAR(255);
ALTER TABLE documents ADD COLUMN payload_type VARCHAR(255);

-- Create indexes for efficient queries on these fields
CREATE INDEX idx_documents_service_type ON documents(service_type);
CREATE INDEX idx_documents_payload_type ON documents(payload_type);
