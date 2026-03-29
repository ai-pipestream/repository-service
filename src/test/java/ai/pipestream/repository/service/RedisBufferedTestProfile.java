package ai.pipestream.repository.service;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/**
 * Test profile that enables redis-buffered storage mode.
 * Used by tests that need the Redis cache layer to be active.
 */
public class RedisBufferedTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("repo.cache.storage-mode", "redis-buffered");
    }
}
