-- Logical document identity: unique per (account_id, datasource_id, document_id)

-- Backfill required for composite uniqueness (V10 added nullable columns)
UPDATE documents SET account_id = '_legacy_unknown' WHERE account_id IS NULL;
UPDATE documents SET datasource_id = '_legacy_unknown' WHERE datasource_id IS NULL;

ALTER TABLE documents ALTER COLUMN account_id SET NOT NULL;
ALTER TABLE documents ALTER COLUMN datasource_id SET NOT NULL;

ALTER TABLE documents DROP CONSTRAINT IF EXISTS documents_document_id_key;

CREATE UNIQUE INDEX uq_documents_account_datasource_document
    ON documents (account_id, datasource_id, document_id);
