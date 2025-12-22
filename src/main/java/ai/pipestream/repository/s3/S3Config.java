package ai.pipestream.repository.s3;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Minimal S3 configuration for Phase 1 HTTP upload.
 *
 * Repo-service is the only S3-facing service; intake proxies bytes and supplies identity via headers.
 */
@ConfigMapping(prefix = "repo.s3")
public interface S3Config {

    String endpoint();

    String region();

    String accessKey();

    String secretKey();

    String bucket();

    /**
     * Whether to use path-style access (required for most MinIO setups).
     */
    @WithDefault("true")
    boolean pathStyleAccess();

    /**
     * Optional object key prefix.
     */
    @WithDefault("uploads")
    String keyPrefix();
}

