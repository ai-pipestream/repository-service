package ai.pipestream.repository.entity;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.vertx.RunOnVertxContext;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    @RunOnVertxContext
    void testCreateAndFindDrive(TransactionalUniAsserter asserter) {
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

        asserter.execute(() -> drive.persist());

        asserter.assertThat(() -> Drive.<Drive>findById(drive.id), foundById -> {
            assertThat(foundById, is(notNullValue()));
            assertThat(foundById.driveId, is(drive.driveId));
            assertThat(foundById.name, is("Test Drive"));
            assertThat(foundById.s3Bucket, is(drive.s3Bucket));
        });

        asserter.assertThat(() -> Drive.<Drive>find("driveId", drive.driveId).firstResult(), foundByDriveId -> {
            assertThat(foundByDriveId, is(notNullValue()));
            assertThat(foundByDriveId.id, is(drive.id));
        });
    }

    @Test
    @RunOnVertxContext
    void testDriveUniqueConstraints(TransactionalUniAsserter asserter) {
        LOG.info("Testing Drive unique constraints");

        // Create first drive
        Drive drive1 = new Drive();
        drive1.driveId = "unique-drive-" + System.currentTimeMillis();
        drive1.name = "Unique Drive";
        drive1.s3Bucket = "unique-bucket-" + System.currentTimeMillis();
        drive1.createdAt = Instant.now();
        drive1.updatedAt = Instant.now();
        asserter.execute(() -> drive1.persist());

        // Try to create drive with same driveId - should fail
        Drive drive2 = new Drive();
        drive2.driveId = drive1.driveId; // Same driveId as drive1
        drive2.name = "Different Name";
        drive2.s3Bucket = "different-bucket-" + System.currentTimeMillis();
        drive2.createdAt = Instant.now();
        drive2.updatedAt = Instant.now();

        asserter.assertFailedWith(drive2::persist, e -> {
            LOG.infof("Correctly caught unique constraint violation: %s", e.getMessage());
            assertThat(e.getMessage(), anyOf(
                    containsString("duplicate"),
                    containsString("unique"),
                    containsString("constraint"),
                    is(nullValue())
            ));
        });
    }

    @Test
    @RunOnVertxContext
    void testDriveUpdateOperations(TransactionalUniAsserter asserter) {
        LOG.info("Testing Drive update operations");

        // Create a drive
        Drive drive = new Drive();
        drive.driveId = "update-drive-" + System.currentTimeMillis();
        drive.name = "Original Name";
        drive.description = "Original description";
        drive.s3Bucket = "update-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

        Instant originalCreated = drive.createdAt;

        drive.name = "Updated Name";
        drive.description = "Updated description";
        drive.s3Prefix = "updated-prefix/";
        drive.updatedAt = Instant.now();

        asserter.execute(() -> Drive.update(
                "name = ?1, description = ?2, s3Prefix = ?3, updatedAt = ?4 where id = ?5",
                drive.name, drive.description, drive.s3Prefix, drive.updatedAt, drive.id));

        asserter.assertThat(() -> Drive.<Drive>findById(drive.id), updatedDrive -> {
            assertThat(updatedDrive, is(notNullValue()));
            assertThat("Name should be updated", updatedDrive.name, is("Updated Name"));
            assertThat("Description should be updated", updatedDrive.description, is("Updated description"));
            assertThat("S3 prefix should be updated", updatedDrive.s3Prefix, is("updated-prefix/"));
            assertThat("Created timestamp unchanged", updatedDrive.createdAt.truncatedTo(ChronoUnit.MICROS),
                    is(originalCreated.truncatedTo(ChronoUnit.MICROS)));
            assertThat("Updated timestamp changed", updatedDrive.updatedAt, is(greaterThanOrEqualTo(originalCreated)));
        });
    }

    @Test
    @RunOnVertxContext
    void testDriveDeleteOperations(TransactionalUniAsserter asserter) {
        LOG.info("Testing Drive delete operations");

        // Create a drive
        Drive drive = new Drive();
        drive.driveId = "delete-drive-" + System.currentTimeMillis();
        drive.name = "Delete Test Drive";
        drive.s3Bucket = "delete-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

        asserter.assertNotNull(() -> Drive.<Drive>findById(drive.id));
        asserter.execute(() -> Drive.deleteById(drive.id));
        asserter.assertNull(() -> Drive.<Drive>findById(drive.id));
    }

    @Test
    @RunOnVertxContext
    void testDriveListOperations(TransactionalUniAsserter asserter) {
        LOG.info("Testing Drive list operations");

        // Create multiple drives
        Drive drive1 = new Drive();
        drive1.driveId = "list-drive-1-" + System.currentTimeMillis();
        drive1.name = "List Drive 1";
        drive1.s3Bucket = "list-bucket-1-" + System.currentTimeMillis();
        drive1.createdAt = Instant.now();
        drive1.updatedAt = Instant.now();
        asserter.execute(() -> drive1.persist());

        Drive drive2 = new Drive();
        drive2.driveId = "list-drive-2-" + System.currentTimeMillis();
        drive2.name = "List Drive 2";
        drive2.s3Bucket = "list-bucket-2-" + System.currentTimeMillis();
        drive2.createdAt = Instant.now();
        drive2.updatedAt = Instant.now();
        asserter.execute(() -> drive2.persist());

        asserter.assertThat(() -> Drive.count(), totalDrives ->
                assertThat("Should have at least 2 drives", totalDrives, is(greaterThanOrEqualTo(2L))));

        asserter.assertThat(() -> Drive.<Drive>listAll(), allDrives ->
                assertThat("Should contain our drives", allDrives.size(), is(greaterThanOrEqualTo(2))));

        asserter.assertThat(() -> Drive.<Drive>list("name like ?1", "List Drive%"), drivesWithName ->
                assertThat("Should find drives with matching names", drivesWithName.size(), is(greaterThanOrEqualTo(2))));
    }
}