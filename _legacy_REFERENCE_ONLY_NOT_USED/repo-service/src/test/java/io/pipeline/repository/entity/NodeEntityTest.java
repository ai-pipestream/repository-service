package io.pipeline.repository.entity;

import io.pipeline.repository.test.EntityTestHelper;
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
public class NodeEntityTest {
    
    private static final Logger LOG = Logger.getLogger(NodeEntityTest.class);
    
    @Inject
    EntityTestHelper entityHelper;
    
    @Test
    @TestTransaction
    void testNodeBasicCrud() {
        LOG.info("Testing Node entity basic CRUD operations");
        
        // Create test drive and node using helper
        Drive testDrive = entityHelper.createTestDrive("node-crud", "node-crud-bucket");
        Node node = entityHelper.createTestNode(testDrive, "test-document", "FILE");
        
        // Verify creation
        assertThat(node.id, is(notNullValue()));
        assertThat(node.isPersistent(), is(true));
        assertThat(node.driveId, is(testDrive.id));
        
        LOG.infof("Created node: id=%d, documentId=%s", node.id, node.documentId);
        
        // Test findByDocumentId
        Node foundByDocId = Node.findByDocumentId(node.documentId);
        assertThat(foundByDocId, is(notNullValue()));
        assertThat(foundByDocId.id, is(node.id));
        assertThat(foundByDocId.name, is(node.name));
        
        // Test findByDriveId
        List<Node> driveNodes = Node.findByDriveId(testDrive.id);
        assertThat(driveNodes, hasSize(1));
        assertThat(driveNodes.get(0).id, is(node.id));
        
        // Test findByDriveIdAndName
        List<Node> namedNodes = Node.findByDriveIdAndName(testDrive.id, node.name);
        assertThat(namedNodes, hasSize(1));
        assertThat(namedNodes.get(0).id, is(node.id));
        
        LOG.infof("✅ All Node CRUD operations working correctly");
    }
    
    @Test
    @TestTransaction
    void testNodeHierarchy() {
        LOG.info("Testing Node hierarchy operations");
        
        // Create test drive and hierarchy using helper
        Drive testDrive = entityHelper.createTestDrive("hierarchy", "hierarchy-bucket");
        EntityTestHelper.NodeHierarchy hierarchy = entityHelper.createNodeHierarchy(
            testDrive, "parent-folder", "child-file.txt");
        
        Node parent = hierarchy.parent;
        Node child = hierarchy.child;
        
        LOG.infof("Created hierarchy: parent.id=%d, child.id=%d", parent.id, child.id);
        
        // Test findByParentId
        List<Node> children = Node.findByParentId(parent.id);
        assertThat("Should find 1 child", children, hasSize(1));
        assertThat("Child should match", children.get(0).id, is(child.id));
        assertThat("Child should have correct name", children.get(0).name, is("child-file.txt"));
        
        // Test findByDriveIdAndParentId for children
        List<Node> driveChildren = Node.findByDriveIdAndParentId(testDrive.id, parent.id);
        assertThat("Should find 1 drive child", driveChildren, hasSize(1));
        assertThat("Drive child should match", driveChildren.get(0).id, is(child.id));
        
        // Test findByDriveIdAndParentId for root nodes (parentId = null)
        List<Node> rootNodes = Node.findByDriveIdAndParentId(testDrive.id, null);
        assertThat("Should find 1 root node", rootNodes, hasSize(1));
        assertThat("Root should be parent", rootNodes.get(0).id, is(parent.id));
        
        LOG.infof("✅ Node hierarchy operations working correctly");
    }
    
    @Test
    @TestTransaction
    void testNodeUniqueConstraints() {
        LOG.info("Testing Node unique constraints");
        
        // Create test drive and first node using helper
        Drive testDrive = entityHelper.createTestDrive("unique-test", "unique-bucket");
        Node node1 = entityHelper.createTestNode(testDrive, "unique-document", "FILE");
        
        // Try to create node with same documentId - should fail
        Node node2 = new Node();
        node2.documentId = node1.documentId; // Same document ID as node1
        node2.driveId = testDrive.id;
        node2.name = "different-name.txt";
        node2.nodeTypeId = node1.nodeTypeId;
        node2.path = "/different-name.txt";
        node2.createdAt = OffsetDateTime.now();
        node2.updatedAt = OffsetDateTime.now();
        
        try {
            node2.persist();
            node2.flush(); // Force constraint check
            assertThat("Should have failed due to unique constraint", false);
        } catch (Exception e) {
            // Expected - unique constraint violation
            LOG.infof("✅ Correctly caught unique constraint violation: %s", e.getMessage());
            assertThat(e.getMessage(), anyOf(
                containsString("Duplicate"),
                containsString("unique"),
                containsString("constraint")
            ));
        }
    }
    
    @Test
    @TestTransaction
    void testNodeUpdateOperations() {
        LOG.info("Testing Node update operations");
        
        // Create test drive and node using helper
        Drive testDrive = entityHelper.createTestDrive("update-test", "update-bucket");
        Node node = entityHelper.createTestNode(testDrive, "original-name", "FILE");
        
        // Set initial metadata for update test
        node.metadata = "{\"version\": 1}";
        node.persist();
        
        Long originalId = node.id;
        OffsetDateTime originalCreated = node.createdAt;
        
        // Update node
        node.name = "updated-name.txt";
        node.path = "/updated-name.txt";
        node.size = 2048L;
        node.touch(); // Update timestamp
        node.metadata = "{\"version\": 2}";
        node.persist();
        
        // Verify updates
        Node updatedNode = Node.findByDocumentId(node.documentId);
        assertThat("ID should be unchanged", updatedNode.id, is(originalId));
        assertThat("Name should be updated", updatedNode.name, is("updated-name.txt"));
        assertThat("Path should be updated", updatedNode.path, is("/updated-name.txt"));
        assertThat("Size should be updated", updatedNode.size, is(2048L));
        assertThat("Metadata should be updated", updatedNode.metadata, is("{\"version\": 2}"));
        assertThat("Created timestamp unchanged", updatedNode.createdAt, is(originalCreated));
        assertThat("Updated timestamp changed", updatedNode.updatedAt, is(greaterThan(originalCreated)));
        
        LOG.infof("✅ Node update operations working correctly");
    }
}
