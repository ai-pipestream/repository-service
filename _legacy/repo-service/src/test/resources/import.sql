-- Initial data for tests (only runs when using drop-and-create)

-- Drive statuses
INSERT INTO drive_status (code, description, is_active) VALUES
('ACTIVE', 'Drive is active and available', true),
('INACTIVE', 'Drive is inactive (lazy delete)', false);

-- Node types
INSERT INTO node_type (code, description, protobuf_type) VALUES
('PIPEDOC', 'Pipeline Document (PipeDoc protobuf)', true),
('PIPESTREAM', 'Pipeline Stream (PipeStream protobuf)', true),
('GRAPH_NODE', 'Graph network node (GraphNode protobuf)', true),
('LLM_MODEL', 'LLM Model for embeddings', false),
('FILE', 'Generic file', false);

-- Upload statuses
INSERT INTO upload_status (code, description, is_final) VALUES
('PENDING', 'Upload pending', false),
('UPLOADING', 'Upload in progress', false),
('COMPLETED', 'Upload completed successfully', true),
('FAILED', 'Upload failed', true);