-- Initialize MySQL database for tests
CREATE DATABASE IF NOT EXISTS apicurio_registry;
GRANT ALL PRIVILEGES ON apicurio_registry.* TO 'pipeline'@'%';
FLUSH PRIVILEGES;
