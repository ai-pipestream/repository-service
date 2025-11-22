package io.pipeline.repository.config;

import io.pipeline.repository.filesystem.CreateDriveRequest;
// TODO: Re-implement with new simple architecture
// import io.pipeline.repository.service.filesystem.DriveService;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.vertx.mutiny.core.eventbus.EventBus;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Dev-only initializer that ensures a set of default drives exist.
 * It does not create S3 buckets in production; this bean is active only in the dev build profile.
 */
@ApplicationScoped
@IfBuildProfile("dev")
public class DevDefaultDrivesInitializer {

    private static final Logger LOG = Logger.getLogger(DevDefaultDrivesInitializer.class);

    // TODO: Re-implement with new simple architecture
    // @Inject
    // DriveService driveService;

    @ConfigProperty(name = "pipeline.repository.default-drives", defaultValue = "pipedocs-drive")
    String defaultDrivesCsv;

    @Inject
    EventBus eventBus;

    void onStart(@Observes StartupEvent ev) {

            List<String> drives = Arrays.stream(defaultDrivesCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());

            if (drives.isEmpty()) {
                return;
            }

            LOG.infof("Ensuring default dev drives exist: %s", String.join(", ", drives));

        eventBus.publish("dev.ensure-drives", String.join(",", drives));
    }
}
