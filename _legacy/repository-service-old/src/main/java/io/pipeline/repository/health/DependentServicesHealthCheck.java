package io.pipeline.repository.health;

import io.smallrye.health.api.AsyncHealthCheck;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * TODO: Re-implement with new simple architecture
 */
@Readiness
@ApplicationScoped
public class DependentServicesHealthCheck implements AsyncHealthCheck {

    // TODO: Re-implement with new simple architecture
    // @Inject
    // S3Operations s3Operations;

    // @Inject
    // S3AsyncClient s3AsyncClient;

    // @Inject
    // DriveService driveService;

    @Override
    public Uni<HealthCheckResponse> call() {
        // TODO: Re-implement with new simple architecture
        return Uni.createFrom().item(HealthCheckResponse.up("dependent-services"));
    }

}
