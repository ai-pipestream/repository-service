package io.pipeline.repository.entity;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Core entity foundation tests - verify the basics are rock solid.
 * These tests focus on the absolute essentials that must work.
 */
@QuarkusTest
@TestTransaction  // Auto-rollback after each test
public class EntityFoundationTest {
    
    private static final Logger LOG = Logger.getLogger(EntityFoundationTest.class);
    
    @Inject
    Drive.DriveService driveService;
    
    @Test
    void testAllLookupTablesExist() {
        LOG.info("Testing all lookup tables have initial data");
        
        // DriveStatus lookup
        DriveStatus active = DriveStatus.findByCode("ACTIVE");
        assertThat("ACTIVE DriveStatus must exist", active, is(notNullValue()));
        assertThat(active.id, is(notNullValue()));
        assertThat(active.isActive, is(true));
        
        DriveStatus inactive = DriveStatus.findByCode("INACTIVE");
        assertThat("INACTIVE DriveStatus must exist", inactive, is(notNullValue()));
        assertThat(inactive.isActive, is(false));
        
        // NodeType lookup
        NodeType fileType = NodeType.findByCode("FILE");
        assertThat("FILE NodeType must exist", fileType, is(notNullValue()));
        assertThat(fileType.id, is(notNullValue()));
        assertThat(fileType.protobufType, is(false));
        
        NodeType pipeDocType = NodeType.findByCode("PIPEDOC");
        assertThat("PIPEDOC NodeType must exist", pipeDocType, is(notNullValue()));
        assertThat(pipeDocType.protobufType, is(true));
        
        // UploadStatus lookup
        UploadStatus pending = UploadStatus.findByCode("PENDING");
        assertThat("PENDING UploadStatus must exist", pending, is(notNullValue()));
        assertThat(pending.isFinal, is(false));
        
        UploadStatus completed = UploadStatus.findByCode("COMPLETED");
        assertThat("COMPLETED UploadStatus must exist", completed, is(notNullValue()));
        assertThat(completed.isFinal, is(true));
        
        LOG.info("✅ All lookup tables have proper initial data");
    }
    
    @Test
    void testDriveBasicCrud() {
        LOG.info("Testing Drive basic CRUD operations");
        
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        
        // CREATE
        Drive drive = new Drive();
        drive.name = "foundation-test-drive";
        drive.bucketName = "foundation-test-bucket";
        drive.accountId = "foundation-customer";
        drive.statusId = activeStatus.id;
        drive.description = "Foundation test drive";
        drive.createdAt = OffsetDateTime.now();
        drive.metadata = "{\"test\": \"foundation\"}";
        drive.persist();
        
        // Verify CREATE
        assertThat("Drive ID must be generated", drive.id, is(notNullValue()));
        assertThat("Drive must be persistent", drive.isPersistent(), is(true));
        
        // READ
        Drive found = driveService.findByName("foundation-test-drive");
        assertThat("Drive must be findable by name", found, is(notNullValue()));
        assertThat("Found drive must have same ID", found.id, is(drive.id));
        assertThat("Found drive must have correct bucket", found.bucketName, is("foundation-test-bucket"));
        
        // UPDATE
        drive.description = "Updated foundation test drive";
        drive.metadata = "{\"test\": \"foundation\", \"updated\": true}";
        drive.persist();
        
        Drive updated = driveService.findByName("foundation-test-drive");
        assertThat("Updated description must persist", updated.description, is("Updated foundation test drive"));
        assertThat("Updated metadata must persist", updated.metadata, containsString("updated"));
        
        // COUNT
        long driveCount = Drive.count();
        assertThat("Drive count must be positive", driveCount, is(greaterThan(0L)));
        
        LOG.infof("✅ Drive CRUD operations working: id=%d", drive.id);
    }
    
    @Test
    void testNodeBasicCrud() {
        LOG.info("Testing Node basic CRUD operations");
        
        // Setup dependencies
        DriveStatus activeStatus = DriveStatus.findByCode("ACTIVE");
        NodeType fileType = NodeType.findByCode("FILE");
        
        Drive drive = new Drive();
        drive.name = "node-test-drive";
        drive.bucketName = "node-test-bucket";
        drive.accountId = "node-customer";
        drive.statusId = activeStatus.id;
        drive.createdAt = OffsetDateTime.now();
        drive.persist();
        
        // CREATE Node
        Node node = new Node();
        node.documentId = "foundation-node-doc";
        node.driveId = drive.id;
        node.name = "foundation-test.txt";
        node.nodeTypeId = fileType.id;
        node.path = "/foundation-test.txt";
        node.contentType = "text/plain";
        node.size = 256L;
        node.s3Key = "foundation-node-doc.pb";
        node.createdAt = OffsetDateTime.now();
        node.updatedAt = OffsetDateTime.now();
        node.metadata = "{\"test\": \"node-foundation\"}";
        node.persist();
        
        // Verify CREATE
        assertThat("Node ID must be generated", node.id, is(notNullValue()));
        assertThat("Node must be persistent", node.isPersistent(), is(true));
        
        // READ
        Node found = Node.findByDocumentId("foundation-node-doc");
        assertThat("Node must be findable by documentId", found, is(notNullValue()));
        assertThat("Found node must have same ID", found.id, is(node.id));
        assertThat("Found node must have correct drive", found.driveId, is(drive.id));
        assertThat("Found node must have correct name", found.name, is("foundation-test.txt"));
        
        // UPDATE
        node.name = "updated-foundation-test.txt";
        node.size = 512L;
        node.touch(); // Update timestamp
        node.persist();
        
        Node updated = Node.findByDocumentId("foundation-node-doc");
        assertThat("Updated name must persist", updated.name, is("updated-foundation-test.txt"));
        assertThat("Updated size must persist", updated.size, is(512L));
        
        // COUNT
        long nodeCount = Node.count();
        assertThat("Node count must be positive", nodeCount, is(greaterThan(0L)));
        
        LOG.infof("✅ Node CRUD operations working: id=%d", node.id);
    }
    
    @Test
    void testIdGenerationIsWorking() {
        LOG.info("Testing ID generation across all entities");
        
        // Test Drive ID generation
        Drive drive1 = new Drive("test-drive-1", "bucket-1", "customer-1");
        drive1.statusId = DriveStatus.findByCode("ACTIVE").id;
        drive1.persist();
        
        Drive drive2 = new Drive("test-drive-2", "bucket-2", "customer-1");
        drive2.statusId = DriveStatus.findByCode("ACTIVE").id;
        drive2.persist();
        
        assertThat("Drive IDs must be auto-generated", drive1.id, is(notNullValue()));
        assertThat("Drive IDs must be auto-generated", drive2.id, is(notNullValue()));
        assertThat("Drive IDs must be different", drive1.id, is(not(drive2.id)));
        assertThat("Drive IDs must be sequential", drive2.id, is(greaterThan(drive1.id)));
        
        // Test Node ID generation
        NodeType fileType = NodeType.findByCode("FILE");
        
        Node node1 = new Node("doc-1", drive1.id, "file1.txt", fileType.id, "/file1.txt");
        node1.persist();
        
        Node node2 = new Node("doc-2", drive1.id, "file2.txt", fileType.id, "/file2.txt");
        node2.persist();
        
        assertThat("Node IDs must be auto-generated", node1.id, is(notNullValue()));
        assertThat("Node IDs must be auto-generated", node2.id, is(notNullValue()));
        assertThat("Node IDs must be different", node1.id, is(not(node2.id)));
        assertThat("Node IDs must be sequential", node2.id, is(greaterThan(node1.id)));
        
        LOG.infof("✅ ID generation working: drive1=%d, drive2=%d, node1=%d, node2=%d", 
            drive1.id, drive2.id, node1.id, node2.id);
    }
}
