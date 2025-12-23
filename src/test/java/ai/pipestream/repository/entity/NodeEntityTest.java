package ai.pipestream.repository.entity;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.vertx.RunOnVertxContext;
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
    @RunOnVertxContext
    void testCreateAndFindNode(TransactionalUniAsserter asserter) {
        LOG.info("Testing Node entity CRUD operations");

        // First create a drive for the node
        Drive drive = new Drive();
        drive.driveId = "node-test-drive-" + System.currentTimeMillis();
        drive.name = "Node Test Drive";
        drive.s3Bucket = "node-test-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

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
        asserter.execute(() -> node.persist());

        asserter.assertThat(() -> Node.<Node>findById(node.id), foundById -> {
            assertThat(foundById, is(notNullValue()));
            assertThat(foundById.nodeId, is(node.nodeId));
            assertThat(foundById.name, is("test-document.txt"));
            assertThat(foundById.status, is("ACTIVE"));
        });

        asserter.assertThat(() -> Node.<Node>find("nodeId", node.nodeId).firstResult(), foundByNodeId -> {
            assertThat(foundByNodeId, is(notNullValue()));
            assertThat(foundByNodeId.id, is(node.id));
        });
    }

    @Test
    @RunOnVertxContext
    void testNodeWithDocumentRelationship(TransactionalUniAsserter asserter) {
        LOG.info("Testing Node with Document relationship");

        // Create drive and document first
        Drive drive = new Drive();
        drive.driveId = "relation-drive-" + System.currentTimeMillis();
        drive.name = "Relation Test Drive";
        drive.s3Bucket = "relation-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

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
        asserter.execute(() -> document.persist());

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
        asserter.execute(() -> node.persist());

        asserter.assertThat(() -> Node.<Node>list("drive", drive), driveNodes ->
                assertThat("Should find nodes for drive", driveNodes.size(), is(greaterThanOrEqualTo(1))));

        asserter.assertThat(() -> Node.<Node>list("document", document), documentNodes ->
                assertThat("Should find nodes for document", documentNodes.size(), is(greaterThanOrEqualTo(1))));
    }

    @Test
    @RunOnVertxContext
    void testNodeUniqueConstraints(TransactionalUniAsserter asserter) {
        LOG.info("Testing Node unique constraints");

        // Create drive first
        Drive drive = new Drive();
        drive.driveId = "constraint-drive-" + System.currentTimeMillis();
        drive.name = "Constraint Test Drive";
        drive.s3Bucket = "constraint-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

        // Create first node
        Node node1 = new Node();
        node1.nodeId = "unique-node-" + System.currentTimeMillis();
        node1.drive = drive;
        node1.name = "unique-document.txt";
        node1.status = "ACTIVE";
        node1.createdAt = Instant.now();
        node1.updatedAt = Instant.now();
        asserter.execute(() -> node1.persist());

        // Try to create node with same nodeId - should fail
        Node node2 = new Node();
        node2.nodeId = node1.nodeId; // Same nodeId as node1
        node2.drive = drive;
        node2.name = "different-name.txt";
        node2.status = "ACTIVE";
        node2.createdAt = Instant.now();
        node2.updatedAt = Instant.now();

        asserter.assertFailedWith(node2::persist, e -> {
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
    void testNodeStatusQueries(TransactionalUniAsserter asserter) {
        LOG.info("Testing Node status-based queries");

        // Create drive first
        Drive drive = new Drive();
        drive.driveId = "status-drive-" + System.currentTimeMillis();
        drive.name = "Status Test Drive";
        drive.s3Bucket = "status-bucket-" + System.currentTimeMillis();
        drive.createdAt = Instant.now();
        drive.updatedAt = Instant.now();
        asserter.execute(() -> drive.persist());

        // Create nodes with different statuses
        Node activeNode = new Node();
        activeNode.nodeId = "active-node-" + System.currentTimeMillis();
        activeNode.drive = drive;
        activeNode.name = "active-document.txt";
        activeNode.status = "ACTIVE";
        activeNode.createdAt = Instant.now();
        activeNode.updatedAt = Instant.now();
        asserter.execute(() -> activeNode.persist());

        Node processingNode = new Node();
        processingNode.nodeId = "processing-node-" + System.currentTimeMillis();
        processingNode.drive = drive;
        processingNode.name = "processing-document.txt";
        processingNode.status = "PROCESSING";
        processingNode.createdAt = Instant.now();
        processingNode.updatedAt = Instant.now();
        asserter.execute(() -> processingNode.persist());

        asserter.assertThat(() -> Node.<Node>list("status", "ACTIVE"), activeNodes -> {
            assertThat("Should find active nodes", activeNodes.size(), is(greaterThanOrEqualTo(1)));
            boolean foundActive = activeNodes.stream()
                    .anyMatch(node -> node.nodeId.equals(activeNode.nodeId));
            assertThat("Should find our active node", foundActive, is(true));
        });

        asserter.assertThat(() -> Node.<Node>list("status", "PROCESSING"), processingNodes -> {
            assertThat("Should find processing nodes", processingNodes.size(), is(greaterThanOrEqualTo(1)));
            boolean foundProcessing = processingNodes.stream()
                    .anyMatch(node -> node.nodeId.equals(processingNode.nodeId));
            assertThat("Should find our processing node", foundProcessing, is(true));
        });
    }
}