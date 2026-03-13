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
 * Real Hibernate CRUD tests for Document entity (no Mockito).
 */
@QuarkusTest
public class DocumentEntityTest {

    private static final Logger LOG = Logger.getLogger(DocumentEntityTest.class);

    @Test
    @RunOnVertxContext
    void testCreateAndFindDocument(TransactionalUniAsserter asserter) {
        LOG.info("Testing Document entity CRUD operations");

        Document document = new Document();
        document.documentId = "test-doc-" + System.currentTimeMillis();
        document.title = "Test Document";
        document.filename = "test-file.txt";
        document.accountId = "test-account";
        document.datasourceId = "test-ds";
        document.content = "This is test content";
        document.contentType = "text/plain";
        document.contentSize = 100L;
        document.storageLocation = "/test/location";
        document.checksum = "abc123";
        document.createdAt = Instant.now();
        document.updatedAt = Instant.now();
        document.version = 1;
        document.status = "ACTIVE";

        asserter.execute(() -> document.persist());

        asserter.assertThat(() -> Document.<Document>findById(document.id), foundById -> {
            assertThat(foundById, is(notNullValue()));
            assertThat(foundById.documentId, is(document.documentId));
            assertThat(foundById.title, is("Test Document"));
            assertThat(foundById.status, is("ACTIVE"));
        });

        asserter.assertThat(() -> Document.<Document>find("documentId", document.documentId).firstResult(), foundByDocumentId -> {
            assertThat(foundByDocumentId, is(notNullValue()));
            assertThat(foundByDocumentId.id, is(document.id));
        });
    }

    @Test
    @RunOnVertxContext
    void testDocumentUniqueConstraints(TransactionalUniAsserter asserter) {
        LOG.info("Testing Document unique constraints");

        Document doc1 = new Document();
        doc1.documentId = "unique-doc-" + System.currentTimeMillis();
        doc1.title = "Unique Document";
        doc1.filename = "unique.txt";
        doc1.accountId = "test-account";
        doc1.datasourceId = "test-ds";
        doc1.contentType = "text/plain";
        doc1.contentSize = 50L;
        doc1.storageLocation = "/unique/location";
        doc1.checksum = "unique123";
        doc1.createdAt = Instant.now();
        doc1.updatedAt = Instant.now();
        doc1.version = 1;
        doc1.status = "ACTIVE";
        asserter.execute(() -> doc1.persist());

        Document doc2 = new Document();
        doc2.documentId = doc1.documentId;
        doc2.title = "Different Title";
        doc2.filename = "diff.txt";
        doc2.accountId = "test-account";
        doc2.datasourceId = "test-ds";
        doc2.contentType = "text/plain";
        doc2.contentSize = 25L;
        doc2.storageLocation = "/different/location";
        doc2.checksum = "different123";
        doc2.createdAt = Instant.now();
        doc2.updatedAt = Instant.now();
        doc2.version = 1;
        doc2.status = "ACTIVE";

        asserter.assertFailedWith(doc2::persist, e -> {
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
    void testDocumentUpdateOperations(TransactionalUniAsserter asserter) {
        LOG.info("Testing Document update operations");

        Document document = new Document();
        document.documentId = "update-doc-" + System.currentTimeMillis();
        document.title = "Original Title";
        document.filename = "original.txt";
        document.accountId = "test-account";
        document.datasourceId = "test-ds";
        document.content = "Original content";
        document.contentType = "text/plain";
        document.contentSize = 100L;
        document.storageLocation = "/original/location";
        document.checksum = "original123";
        document.createdAt = Instant.now();
        document.updatedAt = Instant.now();
        document.version = 1;
        document.status = "ACTIVE";

        asserter.execute(() -> document.persist());

        Instant originalCreated = document.createdAt;

        document.title = "Updated Title";
        document.content = "Updated content";
        document.contentSize = 150L;
        document.version = 2;
        document.updatedAt = Instant.now();

        asserter.execute(() -> Document.update(
                "title = ?1, content = ?2, contentSize = ?3, version = ?4, updatedAt = ?5 where id = ?6",
                document.title, document.content, document.contentSize, document.version, document.updatedAt, document.id));

        asserter.assertThat(() -> Document.<Document>findById(document.id), updatedDoc -> {
            assertThat(updatedDoc, is(notNullValue()));
            assertThat("Title should be updated", updatedDoc.title, is("Updated Title"));
            assertThat("Content should be updated", updatedDoc.content, is("Updated content"));
            assertThat("Size should be updated", updatedDoc.contentSize, is(150L));
            assertThat("Version should be updated", updatedDoc.version, is(2));
            assertThat("Created timestamp unchanged", updatedDoc.createdAt.truncatedTo(ChronoUnit.MICROS),
                    is(originalCreated.truncatedTo(ChronoUnit.MICROS)));
            assertThat("Updated timestamp changed", updatedDoc.updatedAt, is(greaterThanOrEqualTo(originalCreated)));
        });
    }

    @Test
    @RunOnVertxContext
    void testDocumentStatusQueries(TransactionalUniAsserter asserter) {
        LOG.info("Testing Document status-based queries");

        Document activeDoc = new Document();
        activeDoc.documentId = "active-doc-" + System.currentTimeMillis();
        activeDoc.title = "Active Document";
        activeDoc.filename = "active.txt";
        activeDoc.accountId = "test-account";
        activeDoc.datasourceId = "test-ds";
        activeDoc.contentType = "text/plain";
        activeDoc.contentSize = 50L;
        activeDoc.storageLocation = "/active/location";
        activeDoc.checksum = "active123";
        activeDoc.createdAt = Instant.now();
        activeDoc.updatedAt = Instant.now();
        activeDoc.version = 1;
        activeDoc.status = "ACTIVE";
        asserter.execute(() -> activeDoc.persist());

        Document inactiveDoc = new Document();
        inactiveDoc.documentId = "inactive-doc-" + System.currentTimeMillis();
        inactiveDoc.title = "Inactive Document";
        inactiveDoc.filename = "inactive.txt";
        inactiveDoc.accountId = "test-account";
        inactiveDoc.datasourceId = "test-ds";
        inactiveDoc.contentSize = 25L;
        inactiveDoc.storageLocation = "/inactive/location";
        inactiveDoc.checksum = "inactive123";
        inactiveDoc.createdAt = Instant.now();
        inactiveDoc.updatedAt = Instant.now();
        inactiveDoc.version = 1;
        inactiveDoc.status = "DELETED";
        inactiveDoc.contentType = "text/plain";
        asserter.execute(() -> inactiveDoc.persist());

        asserter.assertThat(() -> Document.<Document>list("status", "ACTIVE"), activeDocuments -> {
            assertThat("Should find active documents", activeDocuments.size(), is(greaterThanOrEqualTo(1)));
            boolean foundActive = activeDocuments.stream()
                    .anyMatch(doc -> doc.documentId.equals(activeDoc.documentId));
            assertThat("Should find our active document", foundActive, is(true));
        });

        asserter.assertThat(() -> Document.<Document>list("status", "DELETED"), inactiveDocuments -> {
            assertThat("Should find inactive documents", inactiveDocuments.size(), is(greaterThanOrEqualTo(1)));
            boolean foundInactive = inactiveDocuments.stream()
                    .anyMatch(doc -> doc.documentId.equals(inactiveDoc.documentId));
            assertThat("Should find our inactive document", foundInactive, is(true));
        });
    }
}
