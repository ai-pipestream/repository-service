-- Create drives table
CREATE TABLE drives (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    bucket_name VARCHAR(255) UNIQUE NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    region VARCHAR(50),
    credentials_ref VARCHAR(255),
    status_id BIGINT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    FOREIGN KEY (status_id) REFERENCES drive_status(id),
    INDEX idx_drives_customer (customer_id),
    INDEX idx_drives_status (status_id),
    INDEX idx_drives_name (name)
);
