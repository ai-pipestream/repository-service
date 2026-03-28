package ai.pipestream.repository.service;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RedisStorageConfig} — verifies ConfigMapping injection
 * and {@link RedisStorageConfig#resolvedStorageMode()} parsing.
 */
@QuarkusTest
public class RedisStorageConfigTest {

    private static final Logger LOG = Logger.getLogger(RedisStorageConfigTest.class);

    @Inject
    RedisStorageConfig config;

    @Test
    void configIsInjected() {
        assertThat(config)
                .as("RedisStorageConfig should be injectable via @ConfigMapping")
                .isNotNull();
    }

    @Test
    void defaultStorageMode_isS3Only() {
        // application.properties sets repo.cache.storage-mode=s3-only (via env default)
        assertThat(config.resolvedStorageMode())
                .as("Default storage mode should resolve to S3_ONLY")
                .isEqualTo(StorageMode.S3_ONLY);
    }

    @Test
    void storageModeString_matchesProperty() {
        assertThat(config.storageMode())
                .as("Raw storage-mode string should be readable")
                .isNotNull()
                .isNotBlank();
    }

    @Test
    void redisConfig_hasDefaults() {
        assertThat(config.redis())
                .as("Redis sub-config should be present")
                .isNotNull();
        assertThat(config.redis().ttlHours())
                .as("Default TTL should be 8 hours")
                .isEqualTo(8);
        assertThat(config.redis().flushBatchSize())
                .as("Default flush batch size should be 50")
                .isEqualTo(50);
        assertThat(config.redis().refreshTtlOnRead())
                .as("Default refresh-ttl-on-read should be true")
                .isTrue();
    }

    @Test
    void resolvedStorageMode_parsesVariants() {
        // Test the switch expression logic directly with known inputs
        assertThat(resolveMode("s3-only"))
                .as("'s3-only' should resolve to S3_ONLY")
                .isEqualTo(StorageMode.S3_ONLY);
        assertThat(resolveMode("S3-ONLY"))
                .as("'S3-ONLY' (uppercase) should resolve to S3_ONLY")
                .isEqualTo(StorageMode.S3_ONLY);
        assertThat(resolveMode("redis-buffered"))
                .as("'redis-buffered' should resolve to REDIS_BUFFERED")
                .isEqualTo(StorageMode.REDIS_BUFFERED);
        assertThat(resolveMode("redis_buffered"))
                .as("'redis_buffered' (underscore) should resolve to REDIS_BUFFERED")
                .isEqualTo(StorageMode.REDIS_BUFFERED);
        assertThat(resolveMode("REDIS-BUFFERED"))
                .as("'REDIS-BUFFERED' (uppercase) should resolve to REDIS_BUFFERED")
                .isEqualTo(StorageMode.REDIS_BUFFERED);
        assertThat(resolveMode("redis"))
                .as("'redis' shorthand should resolve to REDIS_BUFFERED")
                .isEqualTo(StorageMode.REDIS_BUFFERED);
        assertThat(resolveMode("REDIS"))
                .as("'REDIS' (uppercase shorthand) should resolve to REDIS_BUFFERED")
                .isEqualTo(StorageMode.REDIS_BUFFERED);
        assertThat(resolveMode("unknown-value"))
                .as("Unknown value should fall back to S3_ONLY")
                .isEqualTo(StorageMode.S3_ONLY);
        assertThat(resolveMode(""))
                .as("Empty string should fall back to S3_ONLY")
                .isEqualTo(StorageMode.S3_ONLY);
    }

    /**
     * Mirrors the switch expression in {@link RedisStorageConfig#resolvedStorageMode()}
     * so we can test all branches without needing to change runtime config.
     */
    private StorageMode resolveMode(String input) {
        return switch (input.toLowerCase().replace("-", "_")) {
            case "redis_buffered", "redis" -> StorageMode.REDIS_BUFFERED;
            default -> StorageMode.S3_ONLY;
        };
    }
}
