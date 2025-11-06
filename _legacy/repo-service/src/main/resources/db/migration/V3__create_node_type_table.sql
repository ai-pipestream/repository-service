-- Create node type lookup table
CREATE TABLE node_type (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255) NOT NULL,
    protobuf_type BOOLEAN DEFAULT false,
    INDEX idx_node_type_code (code)
);

-- Insert initial node types
INSERT INTO node_type (code, description, protobuf_type) VALUES 
('PIPEDOC', 'Pipeline Document (PipeDoc protobuf)', true),
('PIPESTREAM', 'Pipeline Stream (PipeStream protobuf)', true),
('GRAPH_NODE', 'Graph network node (GraphNode protobuf)', true),
('LLM_MODEL', 'LLM Model for embeddings', false),
('FILE', 'Generic file', false);
