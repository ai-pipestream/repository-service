-- Add account_id column to drives table for multi-tenant support
-- First, update existing NULL values to default
UPDATE drives SET account_id = 'default-account' WHERE account_id IS NULL;

-- Then modify the column to be NOT NULL with default
ALTER TABLE drives MODIFY COLUMN account_id VARCHAR(255) NOT NULL DEFAULT 'default-account' COMMENT 'Account ID for multi-tenant support';

-- Create index for account_id lookups (only if it doesn't exist)
-- Check if index exists before creating it
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'drives' AND index_name = 'idx_drives_account_id');
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_drives_account_id ON drives(account_id)', 'SELECT "Index already exists"');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add foreign key constraint (optional, can be added later if needed)
-- ALTER TABLE drives ADD CONSTRAINT fk_drives_account_id FOREIGN KEY (account_id) REFERENCES accounts(account_id);
