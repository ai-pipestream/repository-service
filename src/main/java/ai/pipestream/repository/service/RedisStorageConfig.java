package ai.pipestream.repository.service;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Configuration for the Redis hot cache layer.
 */
@ConfigMapping(prefix = "repo.cache")
public interface RedisStorageConfig {

    @WithName("storage-mode")
    @WithDefault("s3-only")
    String storageMode();

    default StorageMode resolvedStorageMode() {
        return switch (storageMode().toLowerCase().replace("-", "_")) {
            case "redis_buffered", "redis" -> StorageMode.REDIS_BUFFERED;
            default -> StorageMode.S3_ONLY;
        };
    }

    Redis redis();

    interface Redis {
        @WithName("ttl-hours")
        @WithDefault("8")
        int ttlHours();

        @WithName("flush-interval-seconds")
        @WithDefault("5")
        int flushIntervalSeconds();

        @WithName("flush-batch-size")
        @WithDefault("50")
        int flushBatchSize();

        @WithName("refresh-ttl-on-read")
        @WithDefault("true")
        boolean refreshTtlOnRead();
    }
}
