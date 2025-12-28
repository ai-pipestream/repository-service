-- Migration: Add cluster_id column to pipedocs table
-- This enables the new S3 path structure that organizes documents by cluster:
-- - Intake: {prefix}/{account}/{connector}/{datasource}/{docId}/intake/{uuid}.pb (cluster_id = NULL)
-- - Cluster: {prefix}/{account}/{connector}/{datasource}/{docId}/{clusterId}/{uuid}.pb (cluster_id = cluster name)

-- Step 1: Add cluster_id column as nullable
-- Intake documents don't belong to any cluster (cluster_id = NULL)
-- Cluster-processed documents have cluster_id set to the cluster name
ALTER TABLE pipedocs ADD COLUMN cluster_id VARCHAR(255);

-- Step 2: Create index on cluster_id for queries filtering by cluster
CREATE INDEX idx_pipedocs_cluster_id ON pipedocs (cluster_id);

-- Step 3: Create composite index for common query pattern: find all states of a document in a cluster
-- This enables queries like: WHERE doc_id = ? AND cluster_id = ?
CREATE INDEX idx_pipedocs_doc_id_cluster_id ON pipedocs (doc_id, cluster_id);

-- Note: Existing records will have cluster_id = NULL (they're all intake documents from before this migration)
-- This is correct - they should be in the intake/ subdirectory


