package ai.pipestream.repository;

import ai.pipestream.test.support.RepositoryWireMockTestResource;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Bulk storage timing test in {@code s3-only} mode (no Redis cache).
 * <p>
 * Stores and retrieves 102 documents via gRPC, measures timing, and
 * writes results to a temp file for comparison with the Redis-buffered run.
 */
@QuarkusIntegrationTest
@TestProfile(RepositoryIntegrationTestProfile.class)
@QuarkusTestResource(S3TestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
public class BulkStorageS3OnlyIT extends AbstractBulkStorageIT {
    @Override
    String modeName() {
        return "s3-only";
    }
}
