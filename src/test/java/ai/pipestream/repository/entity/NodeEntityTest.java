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
 * Real Hibernate CRUD tests for Node entity (no Mockito).
 * Updated for Reactive Panache.
 */
@QuarkusTest
public class NodeEntityTest {

    private static final Logger LOG = Logger.getLogger(NodeEntityTest.class);

    @Test
    void testCreateAndFindNode() {
        LOG.info("Testing Node entity CRUD operations");

        // First create a drive for the node
        Drive drive = new Drive();
        drive.driveId = "node-test-drive-" + System.currentTimeMillis();
        drive.name = "Node Test Drive";
        drive.s3Bucket = "node-test-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        // Create a new node
        Node node = new Node();
        node.nodeId = "test-node-" + System.currentTimeMillis();
        node.drive = drive;
        node.name = "test-document.txt";
        node.contentType = "text/plain";
        node.sizeBytes = 1024L;
        node.s3Key = "uploads/test-document.txt";
        node.s3Etag = "etag123";
        node.sha256Hash = "hash123";
        node.status = "ACTIVE";
        node.metadata = "{\"test\": true}";
        node.createdAt = Instant.now();
        node.updatedAt = Instant.now();

        // Persist the node
        node::persist).await().indefinitely();;

        // Verify it was saved
        assertThat(node.id, is(notNullValue()));

        LOG.infof("Created node: id=%d, nodeId=%s", node.id, node.nodeId);

        // Test findById
        Node foundById = Node.<Node>findById(node.id).await().indefinitely();
        assertThat(foundById, is(notNullValue()));
        assertThat(foundById.nodeId, is(node.nodeId));
        assertThat(foundById.name, is("test-document.txt"));
        assertThat(foundById.status, is("ACTIVE"));

        // Test find by nodeId
        Node foundByNodeId = Node.<Node>find("nodeId", node.nodeId).firstResult().await().indefinitely();
        assertThat(foundByNodeId, is(notNullValue()));
        assertThat(foundByNodeId.id, is(node.id));

        LOG.infof("All Node lookups working correctly");
    }

    @Test
    void testNodeWithDocumentRelationship() {
        LOG.info("Testing Node with Document relationship");

        // Create drive and document first
        Drive drive = new Drive();
        drive.driveId = "relation-drive-" + System.currentTimeMillis();
        drive.name = "Relation Test Drive";
        drive.s3Bucket = "relation-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        Document document = new Document();
        document.documentId = "relation-doc-" + System.currentTimeMillis();
        document.title = "Related Document";
        document.contentType = "text/plain";
        document.contentSize = 100L;
        document.storageLocation = "/relation/location";
        document.checksum = "relation123";
        document.createdAt = Instant.now();
        document.updatedAt = Instant.now();
        document.version = 1;
        document.status = "ACTIVE";
        document::persist).await().indefinitely();;

        // Create node linked to both drive and document
        Node node = new Node();
        node.nodeId = "relation-node-" + System.currentTimeMillis();
        node.drive = drive;
        node.document = document;
        node.name = "related-document.txt";
        node.contentType = "text/plain";
        node.sizeBytes = 100L;
        node.status = "ACTIVE";
        node.createdAt = Instant.now();
        node.updatedAt = Instant.now();
        node::persist).await().indefinitely();;

        // Verify relationships
        assertThat(node.drive.id, is(drive.id));
        assertThat(node.document.id, is(document.id));

        // Test navigation from drive to nodes
        List<Node> driveNodes = Node.<Node>list("drive", drive).await().indefinitely();
        assertThat("Should find nodes for drive", driveNodes.size(), is(greaterThanOrEqualTo(1)));

        // Test navigation from document to nodes
        List<Node> documentNodes = Node.<Node>list("document", document).await().indefinitely();
        assertThat("Should find nodes for document", documentNodes.size(), is(greaterThanOrEqualTo(1)));

        LOG.infof("Node relationships working correctly");
    }

    @Test
    void testNodeUniqueConstraints() {
        LOG.info("Testing Node unique constraints");

        // Create drive first
        Drive drive = new Drive();
        drive.driveId = "constraint-drive-" + System.currentTimeMillis();
        drive.name = "Constraint Test Drive";
        drive.s3Bucket = "constraint-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        // Create first node
        Node node1 = new Node();
        node1.nodeId = "unique-node-" + System.currentTimeMillis();
        node1.drive = drive;
        node1.name = "unique-document.txt";
        node1.status = "ACTIVE";
        node1.createdAt = Instant.now();
        node1.updatedAt = Instant.now();
        node1::persist).await().indefinitely();;

        // Try to create node with same nodeId - should fail
        Node node2 = new Node();
        node2.nodeId = node1.nodeId; // Same nodeId as node1
        node2.drive = drive;
        node2.name = "different-name.txt";
        node2.status = "ACTIVE";
        node2.createdAt = Instant.now();
        node2.updatedAt = Instant.now();

        try {
            node2::persist).await().indefinitely();;
            assertThat("Should have failed due to unique constraint", false);
        } catch (Exception e) {
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
    void testNodeStatusQueries() {
        LOG.info("Testing Node status-based queries");

        // Create drive first
        Drive drive = new Drive();
        drive.driveId = "status-drive-" + System.currentTimeMillis();
        drive.name = "Status Test Drive";
        drive.s3Bucket = "status-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        drive::persist).await().indefinitely();;

        // Create nodes with different statuses
        Node activeNode = new Node();
        activeNode.nodeId = "active-node-" + System.currentTimeMillis();
        activeNode.drive = drive;
        activeNode.name = "active-document.txt";
        activeNode.status = "ACTIVE";
        activeNode.createdAt = Instant.now();
        activeNode.updatedAt = Instant.now();
        activeNode::persist).await().indefinitely();;

        Node processingNode = new Node();
        processingNode.nodeId = "processing-node-" + System.currentTimeMillis();
        processingNode.drive = drive;
        processingNode.name = "processing-document.txt";
        processingNode.status = "PROCESSING";
        processingNode.createdAt = Instant.now();
        processingNode.updatedAt = Instant.now();
        processingNode::persist).await().indefinitely();;

        // Test find by status
        List<Node> activeNodes = Node.<Node>list("status", "ACTIVE").await().indefinitely();
        assertThat("Should find active nodes", activeNodes.size(), is(greaterThanOrEqualTo(1)));

        List<Node> processingNodes = Node.<Node>list("status", "PROCESSING").await().indefinitely();
        assertThat("Should find processing nodes", processingNodes.size(), is(greaterThanOrEqualTo(1)));

        // Verify our specific nodes are in the results
        boolean foundActive = activeNodes.stream()
            .anyMatch(node -> node.nodeId.equals(activeNode.nodeId));
        boolean foundProcessing = processingNodes.stream()
            .anyMatch(node -> node.nodeId.equals(processingNode.nodeId));

        assertThat("Should find our active node", foundActive, is(true));
        assertThat("Should find our processing node", foundProcessing, is(true));

        LOG.infof("Node status queries working correctly");
    }
}