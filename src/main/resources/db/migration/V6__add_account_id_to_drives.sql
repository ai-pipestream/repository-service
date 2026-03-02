-- Add account_id, region, and credentials_ref to drives table
ALTER TABLE drives ADD COLUMN account_id VARCHAR(255);
ALTER TABLE drives ADD COLUMN region VARCHAR(64);
ALTER TABLE drives ADD COLUMN credentials_ref VARCHAR(512);

CREATE INDEX idx_drives_account_id ON drives (account_id);
ALTER TABLE drives ADD CONSTRAINT uq_drives_account_drive UNIQUE (account_id, drive_id);
