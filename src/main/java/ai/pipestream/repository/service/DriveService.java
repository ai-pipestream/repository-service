package ai.pipestream.repository.service;

import ai.pipestream.repository.entity.Drive;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.s3.S3Config;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for drive resolution, caching, and default drive creation.
 *
 * Drives are logical S3 namespace containers pointing to a bucket + prefix.
 * Most accounts share the platform's default bucket; some may bring their own.
 */
@ApplicationScoped
public class DriveService {

    private static final Logger LOG = Logger.getLogger(DriveService.class);
    private static final String DEFAULT_DRIVE_NAME = "default";

    @Inject
    S3Config s3Config;

    private final ConcurrentHashMap<String, Drive> driveCache = new ConcurrentHashMap<>();

    /**
     * Get or create the default drive for an account.
     * The default drive uses the platform S3 bucket with the accountId as prefix.
     *
     * @param accountId the account ID
     * @return Uni containing the default drive
     */
    public Uni<Drive> getOrCreateDefaultDrive(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("accountId is required"));
        }

        String driveId = accountId + ":" + DEFAULT_DRIVE_NAME;

        // Check cache first
        Drive cached = driveCache.get(driveId);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }

        return Panache.withTransaction(() ->
            Drive.<Drive>find("driveId", driveId).firstResult()
                .flatMap(existing -> {
                    if (existing != null) {
                        driveCache.put(driveId, existing);
                        return Uni.createFrom().item(existing);
                    }

                    // Create default drive
                    Drive drive = new Drive();
                    drive.driveId = driveId;
                    drive.name = DEFAULT_DRIVE_NAME;
                    drive.accountId = accountId;
                    drive.s3Bucket = s3Config.bucket();
                    drive.s3Prefix = accountId;
                    drive.createdAt = Instant.now();
                    drive.updatedAt = Instant.now();

                    return drive.<Drive>persist().invoke(d -> {
                        driveCache.put(driveId, d);
                        LOG.infof("Created default drive for account %s: driveId=%s, bucket=%s, prefix=%s",
                                accountId, driveId, d.s3Bucket, d.s3Prefix);
                    });
                })
        );
    }

    /**
     * Resolve a drive to get the actual S3 bucket and prefix.
     *
     * Falls back to the global S3Config bucket if no Drive entity is found
     * (backward compatibility for existing docs with driveName = "default").
     *
     * @param driveName the drive name (from PipeDocRecord or request)
     * @param accountId the account ID
     * @return Uni containing the resolved drive info
     */
    public Uni<ResolvedDrive> resolveDrive(String driveName, String accountId) {
        // Backward compat: "default" with no account → use global config
        if ((driveName == null || driveName.isBlank() || DEFAULT_DRIVE_NAME.equals(driveName))
                && (accountId == null || accountId.isBlank())) {
            return Uni.createFrom().item(new ResolvedDrive(
                    s3Config.bucket(), s3Config.keyPrefix(), DEFAULT_DRIVE_NAME));
        }

        // Try account-scoped drive first
        String driveId = (driveName != null && !driveName.isBlank() && !DEFAULT_DRIVE_NAME.equals(driveName))
                ? driveName  // Treat as a driveId directly
                : (accountId + ":" + DEFAULT_DRIVE_NAME);

        // Check cache
        Drive cached = driveCache.get(driveId);
        if (cached != null) {
            return Uni.createFrom().item(toResolvedDrive(cached));
        }

        return Drive.<Drive>find("driveId", driveId).firstResult()
                .map(drive -> {
                    if (drive != null) {
                        driveCache.put(driveId, drive);
                        return toResolvedDrive(drive);
                    }
                    // Fallback: use global S3 config (backward compat)
                    LOG.debugf("Drive not found for driveId=%s, falling back to global S3 config", driveId);
                    return new ResolvedDrive(s3Config.bucket(), s3Config.keyPrefix(), DEFAULT_DRIVE_NAME);
                })
                .onFailure().recoverWithItem(e -> {
                    // If no session available (e.g., background context), fall back to global config
                    LOG.debugf("Drive lookup failed for driveId=%s (likely no session): %s, falling back to global S3 config",
                            driveId, e.getMessage());
                    return new ResolvedDrive(s3Config.bucket(), s3Config.keyPrefix(), DEFAULT_DRIVE_NAME);
                });
    }

    /**
     * List drives for a specific account.
     *
     * @param accountId the account ID to filter by
     * @return Uni containing list of drives
     */
    public Uni<List<Drive>> listDrivesForAccount(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return Drive.<Drive>listAll();
        }
        return Drive.<Drive>find("accountId", accountId).list();
    }

    /**
     * Create a new drive entity and cache it.
     *
     * @param drive the drive to persist
     * @return Uni containing the persisted drive
     */
    public Uni<Drive> createDrive(Drive drive) {
        return Panache.withTransaction(() ->
            drive.<Drive>persist().invoke(d -> {
                driveCache.put(d.driveId, d);
                LOG.infof("Created drive: driveId=%s, bucket=%s, accountId=%s",
                        d.driveId, d.s3Bucket, d.accountId);
            })
        );
    }

    /**
     * Find a drive by driveId.
     *
     * @param driveId the drive ID
     * @return Uni containing the drive, or null
     */
    public Uni<Drive> findByDriveId(String driveId) {
        Drive cached = driveCache.get(driveId);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return Drive.<Drive>find("driveId", driveId).firstResult()
                .invoke(d -> {
                    if (d != null) {
                        driveCache.put(driveId, d);
                    }
                });
    }

    /**
     * Delete a drive and all associated PipeDocRecords in a transaction.
     *
     * @param drive the drive entity to delete
     * @return Uni containing the count of PipeDocRecords deleted
     */
    public Uni<Long> deleteDrive(Drive drive) {
        return Panache.withTransaction(() ->
            PipeDocRecord.delete("driveName", drive.driveId)
                .flatMap(deletedCount -> {
                    driveCache.remove(drive.driveId);
                    return drive.<Drive>delete()
                            .replaceWith(deletedCount);
                })
        );
    }

    /**
     * Invalidate cache for a specific drive.
     */
    public void invalidateCache(String driveId) {
        driveCache.remove(driveId);
    }

    private ResolvedDrive toResolvedDrive(Drive drive) {
        String bucket = (drive.s3Bucket != null && !drive.s3Bucket.isBlank())
                ? drive.s3Bucket : s3Config.bucket();
        String prefix = (drive.s3Prefix != null && !drive.s3Prefix.isBlank())
                ? drive.s3Prefix : s3Config.keyPrefix();
        return new ResolvedDrive(bucket, prefix, drive.driveId);
    }

    /**
     * Resolved drive containing the actual S3 bucket and prefix to use.
     */
    public record ResolvedDrive(String bucket, String keyPrefix, String driveId) {}
}
