package ai.pipestream.repository.entity;
import io.quarkus.hibernate.reactive.panache.Panache;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Real Hibernate CRUD tests for Document entity (no Mockito).
 * Updated for Reactive Panache.
 */
@QuarkusTest
public class DocumentEntityTest {

    private static final Logger LOG = Logger.getLogger(DocumentEntityTest.class);

    @Test
    void testCreateAndFindDocument() {
        LOG.info("Testing Document entity CRUD operations");

        // Create a new document
        Document document = new Document();
        document.documentId = "test-doc-" + System.currentTimeMillis();
        document.title = "Test Document";
        document.content = "This is test content";
        document.contentType = "text/plain";
        document.contentSize = 100L;
        document.storageLocation = "/test/location";
        document.checksum = "abc123";
        document.createdAt = Instant.now();
        document.updatedAt = Instant.now();
        document.version = 1;
        document.status = "ACTIVE";

        // Persist the document
        Panache.withTransaction(document::persist).await().indefinitely();

        // Verify it was saved
        assertThat(document.id, is(notNullValue()));

        LOG.infof("Created document: id=%d, documentId=%s", document.id, document.documentId);

        // Test findById
        Document foundById = Document.<Document>findById(document.id).await().indefinitely();
        assertThat(foundById, is(notNullValue()));
        assertThat(foundById.documentId, is(document.documentId));
        assertThat(foundById.title, is("Test Document"));
        assertThat(foundById.status, is("ACTIVE"));

        // Test find by documentId
        Document foundByDocumentId = Document.<Document>find("documentId", document.documentId).firstResult().await().indefinitely();
        assertThat(foundByDocumentId, is(notNullValue()));
        assertThat(foundByDocumentId.id, is(document.id));

        LOG.infof("All Document lookups working correctly");
    }

    @Test
    void testDocumentUniqueConstraints() {
        LOG.info("Testing Document unique constraints");

        // Create first document
        Document doc1 = new Document();
        doc1.documentId = "unique-doc-" + System.currentTimeMillis();
        doc1.title = "Unique Document";
        doc1.contentType = "text/plain";
        doc1.contentSize = 50L;
        doc1.storageLocation = "/unique/location";
        doc1.checksum = "unique123";
        doc1.createdAt = Instant.now();
        doc1.updatedAt = Instant.now();
        doc1.version = 1;
        doc1.status = "ACTIVE";
        doc1::persist).await().indefinitely();;

        // Try to create document with same documentId - should fail
        Document doc2 = new Document();
        doc2.documentId = doc1.documentId; // Same documentId as doc1
        doc2.title = "Different Title";
        doc2.contentType = "text/plain";
        doc2.contentSize = 25L;
        doc2.storageLocation = "/different/location";
        doc2.checksum = "different123";
        doc2.createdAt = Instant.now();
        doc2.updatedAt = Instant.now();
        doc2.version = 1;
        doc2.status = "ACTIVE";

        try {
            doc2::persist).await().indefinitely();;
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
    void testDocumentUpdateOperations() {
        LOG.info("Testing Document update operations");

        // Create a document
        Document document = new Document();
        document.documentId = "update-doc-" + System.currentTimeMillis();
        document.title = "Original Title";
        document.content = "Original content";
        document.contentType = "text/plain";
        document.contentSize = 100L;
        document.storageLocation = "/original/location";
        document.checksum = "original123";
        document.createdAt = Instant.now();
        document.updatedAt = Instant.now();
        document.version = 1;
        document.status = "ACTIVE";
        document::persist).await().indefinitely();;

        Long originalId = document.id;
        Instant originalCreated = document.createdAt;

        // Update the document
        document.title = "Updated Title";
        document.content = "Updated content";
        document.contentSize = 150L;
        document.version = 2;
        document.updatedAt = Instant.now();
        document::persist).await().indefinitely();;

        // Verify updates
        Document updatedDoc = Document.<Document>findById(document.id).await().indefinitely();
        assertThat("ID should be unchanged", updatedDoc.id, is(originalId));
        assertThat("Title should be updated", updatedDoc.title, is("Updated Title"));
        assertThat("Content should be updated", updatedDoc.content, is("Updated content"));
        assertThat("Size should be updated", updatedDoc.contentSize, is(150L));
        assertThat("Version should be updated", updatedDoc.version, is(2));
        assertThat("Created timestamp unchanged", updatedDoc.createdAt, is(originalCreated));
        assertThat("Updated timestamp changed", updatedDoc.updatedAt, is(greaterThanOrEqualTo(originalCreated)));

        LOG.infof("Document update operations working correctly");
    }

    @Test
    void testDocumentStatusQueries() {
        LOG.info("Testing Document status-based queries");

        // Create documents with different statuses
        Document activeDoc = new Document();
        activeDoc.documentId = "active-doc-" + System.currentTimeMillis();
        activeDoc.title = "Active Document";
        activeDoc.contentType = "text/plain";
        activeDoc.contentSize = 50L;
        activeDoc.storageLocation = "/active/location";
        activeDoc.checksum = "active123";
        activeDoc.createdAt = Instant.now();
        activeDoc.updatedAt = Instant.now();
        activeDoc.version = 1;
        activeDoc.status = "ACTIVE";
        activeDoc::persist).await().indefinitely();;

        Document inactiveDoc = new Document();
        inactiveDoc.documentId = "inactive-doc-" + System.currentTimeMillis();
        inactiveDoc.title = "Inactive Document";
        inactiveDoc.contentType = "text/plain";
        inactiveDoc.contentSize = 25L;
        inactiveDoc.storageLocation = "/inactive/location";
        inactiveDoc.checksum = "inactive123";
        inactiveDoc.createdAt = Instant.now();
        inactiveDoc.updatedAt = Instant.now();
        inactiveDoc.version = 1;
        inactiveDoc.status = "DELETED";
        inactiveDoc::persist).await().indefinitely();;

        // Test find by status
        List<Document> activeDocuments = Document.<Document>list("status", "ACTIVE").await().indefinitely();
        assertThat("Should find active documents", activeDocuments.size(), is(greaterThanOrEqualTo(1)));

        List<Document> inactiveDocuments = Document.<Document>list("status", "DELETED").await().indefinitely();
        assertThat("Should find inactive documents", inactiveDocuments.size(), is(greaterThanOrEqualTo(1)));

        // Verify our specific documents are in the results
        boolean foundActive = activeDocuments.stream()
            .anyMatch(doc -> doc.documentId.equals(activeDoc.documentId));
        boolean foundInactive = inactiveDocuments.stream()
            .anyMatch(doc -> doc.documentId.equals(inactiveDoc.documentId));

        assertThat("Should find our active document", foundActive, is(true));
        assertThat("Should find our inactive document", foundInactive, is(true));

        LOG.infof("Document status queries working correctly");
    }
}