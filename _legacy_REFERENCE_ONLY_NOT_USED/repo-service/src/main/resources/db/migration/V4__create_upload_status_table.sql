-- Create upload status lookup table
CREATE TABLE upload_status (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255) NOT NULL,
    is_final BOOLEAN DEFAULT false,
    INDEX idx_upload_status_code (code)
);

-- Insert initial upload statuses
INSERT INTO upload_status (code, description, is_final) VALUES 
('PENDING', 'Upload pending', false),
('UPLOADING', 'Upload in progress', false),
('COMPLETED', 'Upload completed successfully', true),
('FAILED', 'Upload failed', true);
