package ai.pipestream.repository.health;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.agroal.api.AgroalDataSource;

/**
 * Health check for Repository Service.
 * Checks database connectivity and service readiness.
 */
@Readiness
@ApplicationScoped
public class RepositoryHealthCheck implements HealthCheck {

    @Inject
    AgroalDataSource dataSource;

    @Override
    public HealthCheckResponse call() {
        try {
            // Test database connection
            dataSource.getConnection().close();

            return HealthCheckResponse
                .up("repository-service")
                .withData("database", "connected")
                .build();
        } catch (Exception e) {
            return HealthCheckResponse
                .down("repository-service")
                .withData("database", "disconnected")
                .withData("error", e.getMessage())
                .build();
        }
    }
}
