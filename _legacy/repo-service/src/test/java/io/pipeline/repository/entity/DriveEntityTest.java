package io.pipeline.repository.entity;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
@TestTransaction  // Auto-rollback after each test
public class DriveEntityTest {
    
    private static final Logger LOG = Logger.getLogger(DriveEntityTest.class);
    
    @Inject
    Drive.DriveService driveService;
    
    @Test
    void testCreateAndFindDrive() {
        LOG.info("Testing Drive entity CRUD operations");
        
        // Verify DriveStatus exists from migration
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        assertThat(activeStatus, is(notNullValue()));
        assertThat(activeStatus.code, is("ACTIVE"));
        assertThat(activeStatus.isActive, is(true));
        
        // Create a new drive
        Drive drive = new Drive();
        drive.name = "test-drive";
        drive.bucketName = "test-bucket-123";
        drive.accountId = "customer-456";
        drive.region = "us-east-1";
        drive.credentialsRef = "aws-creds-ref";
        drive.statusId = activeStatus.id;
        drive.description = "Test drive for CRUD operations";
        drive.createdAt = OffsetDateTime.now();
        drive.metadata = "{\"env\": \"test\"}";
        
        // Persist the drive
        drive.persist();
        
        // Verify it was saved
        assertThat(drive.id, is(notNullValue()));
        assertThat(drive.isPersistent(), is(true));
        
        LOG.infof("Created drive: id=%d, name=%s", drive.id, drive.name);
        
        // Test findByName
        Drive foundByName = driveService.findByName("test-drive");
        assertThat(foundByName, is(notNullValue()));
        assertThat(foundByName.id, is(drive.id));
        assertThat(foundByName.bucketName, is("test-bucket-123"));
        assertThat(foundByName.accountId, is("customer-456"));
        
        // Test findByBucketName
        Drive foundByBucket = driveService.findByBucketName("test-bucket-123");
        assertThat(foundByBucket, is(notNullValue()));
        assertThat(foundByBucket.id, is(drive.id));
        
        // Test findByAccountId
        List<Drive> customerDrives = driveService.findByAccountId("customer-456");
        assertThat(customerDrives, hasSize(1));
        assertThat(customerDrives.get(0).id, is(drive.id));
        
        LOG.infof("All Drive lookups working correctly");
    }
    
    @Test
    void testDriveUniqueConstraints() {
        LOG.info("Testing Drive unique constraints");
        
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        
        // Create first drive
        Drive drive1 = new Drive("unique-drive", "unique-bucket", "customer-1");
        drive1.statusId = activeStatus.id;
        drive1.persist();
        
        // Try to create drive with same name - should fail
        Drive drive2 = new Drive("unique-drive", "different-bucket", "customer-1");
        drive2.statusId = activeStatus.id;
        
        try {
            drive2.persist();
            // Force flush to trigger constraint
            drive2.flush();
            assertThat("Should have failed due to unique constraint", false);
        } catch (Exception e) {
            // Expected - unique constraint violation
            LOG.infof("Correctly caught unique constraint violation: %s", e.getMessage());
            assertThat(e.getMessage(), containsString("Duplicate"));
        }
    }
    
    @Test
    void testDriveStatusLookup() {
        LOG.info("Testing DriveStatus lookup table");
        
        // Verify initial statuses from migration
        DriveStatus active = DriveStatus.findByCode("ACTIVE");
        assertThat(active, is(notNullValue()));
        assertThat(active.description, is("Drive is active and available"));
        assertThat(active.isActive, is(true));
        
        DriveStatus inactive = DriveStatus.findByCode("INACTIVE");
        assertThat(inactive, is(notNullValue()));
        assertThat(inactive.description, is("Drive is inactive (lazy delete)"));
        assertThat(inactive.isActive, is(false));
        
        // Test findFinalStatuses - none should be final for DriveStatus
        List<DriveStatus> finalStatuses = DriveStatus.find("isActive", false).list();
        assertThat(finalStatuses, hasSize(1));
        assertThat(finalStatuses.get(0).code, is("INACTIVE"));
        
        LOG.infof("DriveStatus lookup working correctly");
    }
}
