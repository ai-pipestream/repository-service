package io.pipeline.repository.config;

import io.pipeline.repository.filesystem.CreateDriveRequest;
// TODO: Re-implement with new simple architecture
// import io.pipeline.repository.service.filesystem.DriveService;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import java.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ApplicationScoped
@IfBuildProfile("dev")
public class DevDefaultDrivesConsumer {

    private static final Logger LOG = Logger.getLogger(DevDefaultDrivesConsumer.class);

    // TODO: Re-implement with new simple architecture
    // @Inject
    // DriveService driveService;

    @GrpcClient("FilesystemService")
    io.pipeline.repository.filesystem.MutinyFilesystemServiceGrpc.MutinyFilesystemServiceStub filesystemService;

    @ConsumeEvent("dev.ensure-drives")
    public Uni<Void> ensureDrives(String csv) {
        List<String> drives = new ArrayList<>();
        if (csv != null && !csv.isBlank()) {
            Arrays.stream(csv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .distinct()
                    .forEach(drives::add);
        }
        if (drives.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        // Chain sequentially to avoid parallel transactions on one event-loop
        Uni<Void> chain = Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofSeconds(1));
        for (String drive : drives) {
            chain = chain.chain(() -> ensureDriveUni(drive));
        }
        return chain;
    }

    private Uni<Void> ensureDriveUni(String driveName) {
        LOG.infof("Ensuring dev drive: %s", driveName);
        return filesystemService.createDrive(
                    CreateDriveRequest.newBuilder()
                        .setName(driveName)
                        .setDescription("Dev default drive")
                        .build())
                .replaceWithVoid()
                .onFailure().retry().withBackOff(Duration.ofMillis(250), Duration.ofSeconds(2)).atMost(10)
                .onFailure().invoke(e -> LOG.warnf(e, "Failed to ensure drive after retries: %s", driveName));
    }
}
