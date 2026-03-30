package ai.pipestream.repository;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/**
 * Test profile for {@code @QuarkusIntegrationTest} runs.
 * <p>
 * The JAR runs with prod profile, so we override config that differs
 * between prod and test: S3 bucket name, service registration, tracing.
 */
public class RepositoryIntegrationTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                // S3TestResource creates bucket "test-bucket", not the prod default "pipestream"
                "repo.s3.bucket", "test-bucket",
                // Disable service registration (no Consul in integration tests)
                "pipestream.registration.enabled", "false"
        );
    }
}
