-- Rename customer_id to account_id for consistency with enterprise terminology
-- This aligns with the connector-intake service which uses account_id

-- Rename column in drives table (documents table doesn't have customer_id)
ALTER TABLE drives
CHANGE COLUMN customer_id account_id VARCHAR(255);

-- Update any indexes that might reference customer_id
-- (MySQL automatically updates indexes when column is renamed with CHANGE COLUMN)