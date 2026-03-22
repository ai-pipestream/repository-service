-- Optional grouping of logical documents (similarity / dedup / cross-corpus links).
-- SOR rows remain keyed by (account_id, datasource_id, document_id); this is additive.

CREATE TABLE document_aggregate (
    aggregate_id UUID PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE TABLE document_aggregate_member (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES document_aggregate(aggregate_id) ON DELETE CASCADE,
    account_id VARCHAR(255) NOT NULL,
    datasource_id VARCHAR(255) NOT NULL,
    document_id VARCHAR(255) NOT NULL,
    joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_document_aggregate_member UNIQUE (aggregate_id, account_id, datasource_id, document_id)
);

CREATE INDEX idx_document_aggregate_member_doc
    ON document_aggregate_member (account_id, datasource_id, document_id);
