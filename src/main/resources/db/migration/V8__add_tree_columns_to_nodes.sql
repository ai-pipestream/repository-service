-- Add tree hierarchy columns to nodes table for parent-child relationships and path-based queries.

ALTER TABLE nodes ADD COLUMN parent_id BIGINT REFERENCES nodes(id);
ALTER TABLE nodes ADD COLUMN path VARCHAR(2048);
ALTER TABLE nodes ADD COLUMN node_type_id BIGINT DEFAULT 2;

CREATE INDEX idx_nodes_parent_id ON nodes(parent_id);
CREATE INDEX idx_nodes_path ON nodes(path);
CREATE INDEX idx_nodes_drive_path ON nodes(drive_id, path);
