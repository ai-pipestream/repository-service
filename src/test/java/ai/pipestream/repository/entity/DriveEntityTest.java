package ai.pipestream.repository.entity;
import io.quarkus.hibernate.reactive.panache.Panache;

import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Real Hibernate CRUD tests for Drive entity (no Mockito).
 * Tests basic create, read, update, delete operations with actual database.
 * Updated for Reactive Panache.
 */
@QuarkusTest
public class DriveEntityTest {

    private static final Logger LOG = Logger.getLogger(DriveEntityTest.class);

    @Test
    void testCreateAndFindDrive() {
        LOG.info("Testing Drive entity CRUD operations");

        // Create a new drive
        Drive drive = new Drive();
        drive.driveId = "test-drive-" + System.currentTimeMillis();
        drive.name = "Test Drive";
        drive.description = "Test drive for CRUD operations";
        drive.s3Bucket = "test-bucket-" + System.currentTimeMillis();
        drive.s3Prefix = "uploads/";
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();

        // Persist the drive
        drive::persist).await().indefinitely();;

        // Verify it was saved
        assertThat(drive.id, is(notNullValue()));

        LOG.infof("Created drive: id=%d, driveId=%s", drive.id, drive.driveId);

        // Test findById
        Drive foundById = Drive.<Drive>findById(drive.id).await().indefinitely();
        assertThat(foundById, is(notNullValue()));
        assertThat(foundById.driveId, is(drive.driveId));
        assertThat(foundById.name, is("Test Drive"));
        assertThat(foundById.s3Bucket, is(drive.s3Bucket));

        // Test find by driveId
        Drive foundByDriveId = Drive.<Drive>find("driveId", drive.driveId).firstResult().await().indefinitely();
        assertThat(foundByDriveId, is(notNullValue()));
        assertThat(foundByDriveId.id, is(drive.id));

        LOG.infof("All Drive lookups working correctly");
    }

    @Test
    void testDriveUniqueConstraints() {
        LOG.info("Testing Drive unique constraints");

        // Create first drive
        Drive drive1 = new Drive();
        drive1.driveId = "unique-drive-" + System.currentTimeMillis();
        drive1.name = "Unique Drive";
        drive1.s3Bucket = "unique-bucket-" + System.currentTimeMillis();
        drive1.createdAt = Instant.now();
        drive1.updatedAt = Instant.now();
        drive1::persist).await().indefinitely();;

        // Try to create drive with same driveId - should fail
        Drive drive2 = new Drive();
        drive2.driveId = drive1.driveId; // Same driveId as drive1
        drive2.name = "Different Name";
        drive2.s3Bucket = "different-bucket-" + System.currentTimeMillis();
        drive2.createdAt = Instant.now();
        drive2.updatedAt = Instant.now();

        try {
            drive2::persist).await().indefinitely();;
            assertThat("Should have failed due to unique constraint", false);
        } catch (Exception e) {
            // Expected - unique constraint violation
            LOG.infof("Correctly caught unique constraint violation: %s", e.getMessage());
            assertThat(e.getMessage(), anyOf(
                containsString("duplicate"),
                containsString("unique"),
                containsString("constraint"),
                is(nullValue())
            ));
        }
    }

    @Test
    void testDriveUpdateOperations() {
        LOG.info("Testing Drive update operations");

        // Create a drive
        Drive drive = new Drive();
        drive.driveId = "update-drive-" + System.currentTimeMillis();
        drive.name = "Original Name";
        drive.description = "Original description";
        drive.s3Bucket = "update-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        Long originalId = drive.id;
        Instant originalCreated = drive.createdAt;

        // Update the drive
        drive.name = "Updated Name";
        drive.description = "Updated description";
        drive.s3Prefix = "updated-prefix/";
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        // Verify updates
        Drive updatedDrive = Drive.<Drive>findById(drive.id).await().indefinitely();
        assertThat("ID should be unchanged", updatedDrive.id, is(originalId));
        assertThat("Name should be updated", updatedDrive.name, is("Updated Name"));
        assertThat("Description should be updated", updatedDrive.description, is("Updated description"));
        assertThat("S3 prefix should be updated", updatedDrive.s3Prefix, is("updated-prefix/"));
        assertThat("Created timestamp unchanged", updatedDrive.createdAt, is(originalCreated));
        assertThat("Updated timestamp changed", updatedDrive.updatedAt, is(greaterThanOrEqualTo(originalCreated)));

        LOG.infof("Drive update operations working correctly");
    }

    @Test
    void testDriveDeleteOperations() {
        LOG.info("Testing Drive delete operations");

        // Create a drive
        Drive drive = new Drive();
        drive.driveId = "delete-drive-" + System.currentTimeMillis();
        drive.name = "Delete Test Drive";
        drive.s3Bucket = "delete-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        Long driveId = drive.id;

        // Verify it exists
        assertThat(Drive.<Drive>findById(driveId).await().indefinitely(), is(notNullValue()));

        // Delete the drive
        drive.delete().await().indefinitely();

        // Verify it's gone
        assertThat(Drive.<Drive>findById(driveId).await().indefinitely(), is(nullValue()));

        LOG.infof("Drive delete operations working correctly");
    }

    @Test
    void testDriveListOperations() {
        LOG.info("Testing Drive list operations");

        // Create multiple drives
        Drive drive1 = new Drive();
        drive1.driveId = "list-drive-1-" + System.currentTimeMillis();
        drive1.name = "List Drive 1";
        drive1.s3Bucket = "list-bucket-1-" + System.currentTimeMillis();
        drive1.createdAt = Instant.now();
        drive1.updatedAt = Instant.now();
        drive1::persist).await().indefinitely();;

        Drive drive2 = new Drive();
        drive2.driveId = "list-drive-2-" + System.currentTimeMillis();
        drive2.name = "List Drive 2";
        drive2.s3Bucket = "list-bucket-2-" + System.currentTimeMillis();
        drive2.createdAt = Instant.now();
        drive2.updatedAt = Instant.now();
        drive2::persist).await().indefinitely();;

        // Test count
        long totalDrives = Drive.count().await().indefinitely();
        assertThat("Should have at least 2 drives", totalDrives, is(greaterThanOrEqualTo(2L)));

        // Test list all
        List<Drive> allDrives = Drive.<Drive>listAll().await().indefinitely();
        assertThat("Should contain our drives", allDrives.size(), is(greaterThanOrEqualTo(2)));

        // Test find with conditions
        List<Drive> drivesWithName = Drive.<Drive>list("name like ?1", "List Drive%").await().indefinitely();
        assertThat("Should find drives with matching names", drivesWithName.size(), is(greaterThanOrEqualTo(2)));

        LOG.infof("Drive list operations working correctly");
    }
}