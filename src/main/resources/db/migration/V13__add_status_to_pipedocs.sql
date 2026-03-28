-- Add status column for Redis hot cache lifecycle tracking.
-- AVAILABLE: document is in durable storage (default, backward-compatible).
-- PENDING_STORAGE: document is in Redis, awaiting async flush to durable storage.
-- LOST: Redis TTL expired before flush completed.
ALTER TABLE pipedocs ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE';

-- Index for the background flusher to find pending documents efficiently.
CREATE INDEX IF NOT EXISTS idx_pipedocs_status ON pipedocs (status) WHERE status != 'AVAILABLE';
