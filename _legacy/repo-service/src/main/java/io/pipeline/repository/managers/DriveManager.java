package io.pipeline.repository.managers;

import io.pipeline.repository.entity.Drive;
import io.pipeline.repository.entity.DriveStatus;
import io.pipeline.repository.exception.DriveNotFoundException;
import io.pipeline.repository.service.AccountValidationService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.enterprise.context.control.ActivateRequestContext;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import java.time.OffsetDateTime;

/**
 * Manages Drive creation, S3 bucket creation, and account relationships.
 * <p>
 * Account validation is performed via gRPC calls to account-manager service.
 */
@ApplicationScoped
public class DriveManager {

    private static final Logger LOG = Logger.getLogger(DriveManager.class);

    @Inject
    S3Client s3Client;

    @Inject
    AccountValidationService accountValidationService;

    @Inject
    Drive.DriveService driveService;

    /**
     * Create drive with optional S3 bucket creation.
     * <p>
     * Validates account via gRPC call to account-manager before creating drive.
     */
    @ActivateRequestContext
    public Uni<Drive> createDrive(String driveName, String bucketName, String accountId,
                                 String region, String description, boolean createBucket) {
        LOG.infof("Creating drive: drive=%s, bucket=%s, account=%s, createBucket=%s",
            driveName, bucketName, accountId, createBucket);

        // 1. Validate account via gRPC (async)
        return accountValidationService.validateAccountExistsAndActive(accountId)
            .flatMap(ignored -> Uni.createFrom().item(() ->
                createDriveTransactional(driveName, bucketName, accountId, region, description, createBucket)
            ).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()));
    }

    @Transactional
    public Drive createDriveTransactional(String driveName, String bucketName, String accountId,
                                        String region, String description, boolean createBucket) {
        // 1. Check if drive already exists
        Drive existingDrive = driveService.findByName(driveName);
        if (existingDrive != null) {
            throw new RuntimeException("Drive already exists: " + driveName);
        }

        // 2. Create S3 bucket if requested
        if (createBucket) {
            createS3BucketIfNotExists(bucketName, region);
        } else {
            // Verify bucket exists if not creating
            if (!bucketExists(bucketName)) {
                throw new RuntimeException("S3 bucket does not exist: " + bucketName);
            }
        }

        // 3. Get ACTIVE status
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        if (activeStatus == null) {
            throw new RuntimeException("ACTIVE drive status not found");
        }

        // 4. Create Drive entity
        Drive drive = new Drive();
        drive.name = driveName;
        drive.bucketName = bucketName;
        drive.accountId = accountId;
        drive.region = region;
        drive.statusId = activeStatus.id;
        drive.description = description;
        drive.createdAt = OffsetDateTime.now();
        drive.persist();

        LOG.infof("Drive created successfully: id=%d, name=%s, bucket=%s",
            drive.id, driveName, bucketName);

        return drive;
    }

    /**
     * Create drive using existing S3 bucket.
     * <p>
     * Validates account via gRPC call to account-manager before creating drive.
     */
    @ActivateRequestContext
    public Uni<Drive> createDrive(String driveName, String bucketName, String accountId,
                                 String region, String description) {
        LOG.infof("Creating drive with existing bucket: drive=%s, bucket=%s, account=%s",
            driveName, bucketName, accountId);

        // 1. Validate account via gRPC (async)
        return accountValidationService.validateAccountExistsAndActive(accountId)
            .flatMap(ignored -> Uni.createFrom().item(() ->
                createDriveTransactional(driveName, bucketName, accountId, region, description, false)
            ).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()));
    }

    /**
     * Get drive by name.
     */
    public Uni<Drive> getDrive(String driveName) {
        return Uni.createFrom().item(() -> {
            Drive drive = driveService.findByName(driveName);
            if (drive == null) {
                throw new DriveNotFoundException(driveName);
            }
            return drive;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private void createS3BucketIfNotExists(String bucketName, String region) {
        try {
            if (!bucketExists(bucketName)) {
                LOG.infof("Creating S3 bucket: %s in region %s", bucketName, region);

                CreateBucketRequest.Builder requestBuilder = CreateBucketRequest.builder()
                    .bucket(bucketName);

                // Only set CreateBucketConfiguration for regions other than us-east-1
                if (region != null && !region.equals("us-east-1")) {
                    requestBuilder.createBucketConfiguration(builder ->
                        builder.locationConstraint(region));
                }

                s3Client.createBucket(requestBuilder.build());
                LOG.infof("S3 bucket created successfully: %s", bucketName);
            } else {
                LOG.infof("S3 bucket already exists: %s", bucketName);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Failed to create S3 bucket: %s", bucketName);
            throw new RuntimeException("Failed to create S3 bucket: " + bucketName, e);
        }
    }

    private boolean bucketExists(String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}