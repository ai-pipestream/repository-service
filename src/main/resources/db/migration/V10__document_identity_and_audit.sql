-- Migration: Evolve logical document identity and link to audit trail
-- This refines the legacy documents table to serve as the "Card Catalog" identity

-- Step 1: Add missing columns to documents table
-- These columns allow the identity record to store current high-level metadata and security posture
ALTER TABLE documents ADD COLUMN IF NOT EXISTS filename VARCHAR(1024);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS account_id VARCHAR(255);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS datasource_id VARCHAR(255);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS acls TEXT[];

-- Step 2: Add GIN index for ACLs on the identity record for efficient browsing
CREATE INDEX IF NOT EXISTS idx_documents_acls ON documents USING GIN (acls);

-- Step 3: Add index on account_id for multi-tenant document listing
CREATE INDEX IF NOT EXISTS idx_documents_account_id ON documents (account_id);

-- Step 4: Ensure documents.document_id is indexed (should be from V1, but reinforcing)
-- This is our logical identity key that ties all PipeDocRecords together
CREATE INDEX IF NOT EXISTS idx_documents_logical_id ON documents (document_id);

-- Step 5: Update pipedocs table to link back to the logical identity
-- This enables a clean join: Document (Identity) -> repeated PipeDocRecord (Audit Trail)
CREATE INDEX IF NOT EXISTS idx_pipedocs_doc_id ON pipedocs (doc_id);

-- Note: Existing records in pipedocs can now be joined to documents using doc_id.
-- Identity records in 'documents' will be populated automatically by DocumentStorageService.
