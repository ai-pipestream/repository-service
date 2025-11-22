-- Create drive status lookup table
CREATE TABLE drive_status (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    INDEX idx_drive_status_code (code)
);

-- Insert initial status values
INSERT INTO drive_status (code, description, is_active) VALUES 
('ACTIVE', 'Drive is active and available', true),
('INACTIVE', 'Drive is inactive (lazy delete)', false);
