-- Migration: Add UUID node_id (as PK) and graph_address_id to pipedocs table
-- The node_id (UUID) is the repository's unique identifier for each document state
-- It serves as the primary key and can be used as Kafka key
-- Multiple entries per doc_id are now supported (one per graph_address_id)

-- Step 1: Add node_id UUID column (this will become the primary key)
ALTER TABLE pipedocs ADD COLUMN node_id UUID;

-- Step 2: Add graph_address_id column first (before generating UUIDs)
ALTER TABLE pipedocs ADD COLUMN graph_address_id VARCHAR(255);

-- Step 3: For existing records, use doc_id as fallback for graph_address_id (initial state)
UPDATE pipedocs SET graph_address_id = doc_id WHERE graph_address_id IS NULL;

-- Step 4: Generate deterministic UUIDs for existing records using (doc_id, graph_address_id, account_id)
-- Note: PostgreSQL doesn't have UUID.nameUUIDFromBytes equivalent, so this uses md5 hash converted to UUID format.
-- The actual deterministic UUID generation for new records happens in Java code (PipeDocUuidGenerator)
-- using UUID.nameUUIDFromBytes(). This SQL approximation will be different from Java's UUID v5, but
-- is deterministic and sufficient for existing records (they will be migrated to proper UUIDs via
-- application code if needed).
-- For a production migration with existing data, consider a Java-based migration script to ensure
-- exact UUID v5 compatibility with the Java code.
UPDATE pipedocs SET node_id = CAST(
    substr(md5(doc_id || '|' || graph_address_id || '|' || account_id), 1, 8) || '-' ||
    substr(md5(doc_id || '|' || graph_address_id || '|' || account_id), 9, 4) || '-' ||
    substr(md5(doc_id || '|' || graph_address_id || '|' || account_id), 13, 4) || '-' ||
    substr(md5(doc_id || '|' || graph_address_id || '|' || account_id), 17, 4) || '-' ||
    substr(md5(doc_id || '|' || graph_address_id || '|' || account_id), 21, 12)
    AS UUID
) WHERE node_id IS NULL;

-- Step 5: Make node_id NOT NULL
ALTER TABLE pipedocs ALTER COLUMN node_id SET NOT NULL;

-- Step 6: Make graph_address_id NOT NULL after backfilling
ALTER TABLE pipedocs ALTER COLUMN graph_address_id SET NOT NULL;

-- Step 7: Remove the old unique constraint on doc_id (now doc_id can have multiple entries)
ALTER TABLE pipedocs DROP CONSTRAINT IF EXISTS pipedocs_doc_id_key;

-- Step 8: Drop the old primary key (auto-increment id)
ALTER TABLE pipedocs DROP CONSTRAINT IF EXISTS pipedocs_pkey;

-- Step 9: Drop the old id column (no longer needed)
ALTER TABLE pipedocs DROP COLUMN IF EXISTS id;

-- Step 10: Make node_id the primary key
ALTER TABLE pipedocs ADD PRIMARY KEY (node_id);

-- Step 11: Add composite index for DocumentReference lookups (doc_id, graph_address_id, account_id)
-- This enables efficient lookups via GetPipeDocByReference RPC
CREATE INDEX idx_pipedocs_doc_graph_address_account 
    ON pipedocs (doc_id, graph_address_id, account_id);

-- Step 12: Add index on graph_address_id for queries filtering by graph location
CREATE INDEX idx_pipedocs_graph_address_id ON pipedocs (graph_address_id);

-- Step 13: Keep existing indexes (they're still useful for queries)
-- idx_pipedocs_doc_id (now non-unique, but still indexed for doc_id lookups)
-- idx_pipedocs_account_id
-- idx_pipedocs_datasource_id
-- idx_pipedocs_checksum
-- idx_pipedocs_created_at

