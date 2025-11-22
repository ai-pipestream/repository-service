package io.pipeline.repository.config;

import io.pipeline.repository.entity.Drive;
import io.pipeline.repository.entity.DriveStatus;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.transaction.Transactional;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import io.quarkus.arc.profile.IfBuildProfile;

/**
 * Dev and Test initializer that creates default drives on startup.
 * This ensures we have test drives available for development and testing.
 */
@ApplicationScoped
@IfBuildProfile(anyOf = {"dev", "test"})
public class DevDriveInitializer {

    private static final Logger LOG = Logger.getLogger(DevDriveInitializer.class);
    
    @Inject
    Drive.DriveService driveService;

    @Transactional
    void onStart(@Observes StartupEvent ev) {
        LOG.info("Initializing default development drives...");

        try {
            // Ensure ACTIVE status exists
            DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
            if (activeStatus == null) {
                activeStatus = new DriveStatus();
                activeStatus.code = "ACTIVE";
                activeStatus.description = "Active drive status";
                activeStatus.isActive = true;
                activeStatus.persist();
                LOG.infof("Created ACTIVE drive status");
            }

            // Create test-drive for testing chunked uploads
            Drive testDrive = driveService.findByName("test-drive");
            if (testDrive == null) {
                testDrive = new Drive();
                testDrive.name = "test-drive";
                testDrive.bucketName = "modules-drive";  // Use existing MinIO bucket
                testDrive.accountId = "test-account";
                testDrive.description = "Test drive for development";
                testDrive.statusId = activeStatus.id;
                testDrive.region = "us-east-1";
                testDrive.persist();
                LOG.infof("Created test-drive with bucket: modules-drive");
            } else {
                LOG.infof("test-drive already exists");
            }

            // Create pipedocs-drive for documents
            Drive pipedocsDrive = driveService.findByName("pipedocs-drive");
            if (pipedocsDrive == null) {
                pipedocsDrive = new Drive();
                pipedocsDrive.name = "pipedocs-drive";
                pipedocsDrive.bucketName = "pipedocs";
                pipedocsDrive.accountId = "default-account";
                pipedocsDrive.description = "Drive for pipeline documents";
                pipedocsDrive.statusId = activeStatus.id;
                pipedocsDrive.region = "us-east-1";
                pipedocsDrive.persist();
                LOG.infof("Created pipedocs-drive with bucket: pipedocs");
            } else {
                LOG.infof("pipedocs-drive already exists");
            }

        } catch (Exception e) {
            LOG.errorf(e, "Failed to initialize development drives");
        }
    }
}