package io.pipeline.repository.managers;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Manages S3 path construction with connector organization.
 * Handles the standard: {drive}/{connector-name}/{path}/{uuid}.pb
 */
@ApplicationScoped
public class PathManager {

    private static final Logger LOG = Logger.getLogger(PathManager.class);
    private static final String PROTOBUF_EXTENSION = ".pb";

    /**
     * Build S3 key with connector organization.
     * Format: connectors/{connector-name}/{path}/{uuid}.pb
     */
    public String buildS3Key(String connectorId, String path, String nodeId) {
        StringBuilder s3Key = new StringBuilder();

        // Start with connectors prefix
        s3Key.append("connectors/");

        // Add connector ID
        if (connectorId != null && !connectorId.isEmpty()) {
            s3Key.append(connectorId);
        } else {
            s3Key.append("default");
        }

        // Add path if provided
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            // Normalize path - remove leading/trailing slashes
            String normalizedPath = path.replaceAll("^/+|/+$", "");
            if (!normalizedPath.isEmpty()) {
                s3Key.append("/").append(normalizedPath);
            }
        }

        // Add node ID with extension
        s3Key.append("/").append(nodeId).append(PROTOBUF_EXTENSION);

        String result = s3Key.toString();
        LOG.debugf("Built S3 key: connectorId=%s, path=%s, nodeId=%s → %s",
                  connectorId, path, nodeId, result);

        return result;
    }

    /**
     * Extract components from S3 key.
     */
    public S3KeyComponents parseS3Key(String s3Key) {
        if (s3Key == null || !s3Key.startsWith("connectors/")) {
            throw new IllegalArgumentException("Invalid S3 key format: " + s3Key);
        }

        // Remove connectors/ prefix and .pb extension
        String withoutPrefix = s3Key.substring("connectors/".length());
        String withoutExtension = withoutPrefix.endsWith(PROTOBUF_EXTENSION) ?
            withoutPrefix.substring(0, withoutPrefix.length() - PROTOBUF_EXTENSION.length()) : withoutPrefix;

        String[] parts = withoutExtension.split("/");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid S3 key format: " + s3Key);
        }

        String connectorId = parts[0];
        String nodeId = parts[parts.length - 1]; // Last part is always nodeId

        // Build path from middle parts
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 1; i < parts.length - 1; i++) {
            if (pathBuilder.length() > 0) {
                pathBuilder.append("/");
            }
            pathBuilder.append(parts[i]);
        }
        String path = pathBuilder.length() > 0 ? pathBuilder.toString() : "";

        return new S3KeyComponents(connectorId, path, nodeId);
    }

    /**
     * Validate connector ID format.
     */
    public boolean isValidConnectorId(String connectorId) {
        if (connectorId == null || connectorId.isEmpty()) {
            return false;
        }
        // Allow alphanumeric, hyphens, underscores
        return connectorId.matches("^[a-zA-Z0-9_-]+$");
    }

    /**
     * Validate path format.
     */
    public boolean isValidPath(String path) {
        if (path == null) {
            return true; // null is valid (root)
        }
        // Allow reasonable path characters
        return path.matches("^[a-zA-Z0-9_/-]*$");
    }

    /**
     * Components of an S3 key.
     */
    public static class S3KeyComponents {
        public final String connectorId;
        public final String path;
        public final String nodeId;

        public S3KeyComponents(String connectorId, String path, String nodeId) {
            this.connectorId = connectorId;
            this.path = path;
            this.nodeId = nodeId;
        }
    }
}