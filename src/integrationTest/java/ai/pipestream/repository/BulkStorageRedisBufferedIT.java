package ai.pipestream.repository;

import ai.pipestream.test.support.RepositoryWireMockTestResource;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.util.HashMap;
import java.util.Map;

/**
 * Bulk storage timing test in {@code redis-buffered} mode (Redis L1 cache).
 * <p>
 * Stores and retrieves 102 documents via gRPC, measures timing, and
 * writes results to a temp file for comparison with the S3-only run.
 */
@QuarkusIntegrationTest
@TestProfile(BulkStorageRedisBufferedIT.RedisBufferedProfile.class)
@QuarkusTestResource(S3TestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
public class BulkStorageRedisBufferedIT extends AbstractBulkStorageIT {
    @Override
    String modeName() {
        return "redis-buffered";
    }

    public static class RedisBufferedProfile extends RepositoryIntegrationTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>(super.getConfigOverrides());
            config.put("repo.cache.storage-mode", "redis-buffered");
            return config;
        }
    }
}
