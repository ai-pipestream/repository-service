-- Migration: Add acls column to pipedocs table for document-level security
-- This aligns with the new ACLs field in PipeDoc.OwnershipContext

-- Step 1: Add acls column as a text array
-- Using text[] allows for efficient multi-value storage and querying
ALTER TABLE pipedocs ADD COLUMN acls TEXT[];

-- Step 2: Create a GIN index for efficient ACL filtering
-- This enables fast lookups for documents a user is authorized to see
-- Query example: WHERE acls && ARRAY['user:123', 'group:admin']
CREATE INDEX idx_pipedocs_acls ON pipedocs USING GIN (acls);

-- Note: Existing records will have acls = NULL (open/default access)
