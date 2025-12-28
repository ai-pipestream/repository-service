package ai.pipestream.repository.entity;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.vertx.RunOnVertxContext;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Real Hibernate CRUD tests for PipeDocRecord entity (no Mockito).
 * Updated for Reactive Panache.
 */
@QuarkusTest
public class PipeDocRecordEntityTest {

    private static final Logger LOG = Logger.getLogger(PipeDocRecordEntityTest.class);

    @Inject
    PipeDocUuidGenerator uuidGenerator;

    @Test
    @RunOnVertxContext
    void testCreateAndFindPipeDocRecord(TransactionalUniAsserter asserter) {
        LOG.info("Testing PipeDocRecord entity CRUD operations");

        // Create a new PipeDocRecord (metadata only, no pipedoc bytes in DB)
        PipeDocRecord record = new PipeDocRecord();
        record.docId = "test-doc-" + System.currentTimeMillis();
        record.graphAddressId = "node-1"; // Graph node ID
        record.accountId = "acc-test";
        record.datasourceId = "ds-test";
        record.checksum = "checksum123";
        record.driveName = "test-drive";
        record.objectKey = "uploads/test-document.txt";
        record.pipedocObjectKey = "uploads/test-document.txt.pipedoc";
        record.versionId = "v1.0";
        record.etag = "etag123";
        record.sizeBytes = 1024L;
        record.contentType = "text/plain";
        record.filename = "test-document.txt";
        record.createdAt = Instant.now();
        
        // Generate deterministic UUID for node_id
        record.nodeId = uuidGenerator.generateNodeId(record.docId, record.graphAddressId, record.accountId);

        // Persist the record
        asserter.execute(() -> record.persist());

        asserter.assertThat(() -> PipeDocRecord.<PipeDocRecord>findById(record.nodeId), foundById -> {
            assertThat(foundById, is(notNullValue()));
            assertThat(foundById.docId, is(record.docId));
            assertThat(foundById.graphAddressId, is(record.graphAddressId));
            assertThat(foundById.checksum, is("checksum123"));
            assertThat(foundById.sizeBytes, is(1024L));
        });

        asserter.assertThat(() -> PipeDocRecord.<PipeDocRecord>find("docId = ?1 and graphAddressId = ?2 and accountId = ?3", 
                record.docId, record.graphAddressId, record.accountId).firstResult(), foundByComposite -> {
            assertThat(foundByComposite, is(notNullValue()));
            assertThat(foundByComposite.nodeId, is(record.nodeId));
        });
    }

    @Test
    @RunOnVertxContext
    void testPipeDocRecordUniqueConstraints(TransactionalUniAsserter asserter) {
        LOG.info("Testing PipeDocRecord unique constraints");

        // Create first record
        PipeDocRecord record1 = new PipeDocRecord();
        record1.docId = "unique-doc-" + System.currentTimeMillis();
        record1.graphAddressId = "node-1";
        record1.accountId = "acc-unique";
        record1.datasourceId = "ds-unique";
        record1.checksum = "unique123";
        record1.driveName = "unique-drive";
        record1.objectKey = "uploads/unique-document.txt";
        record1.pipedocObjectKey = "uploads/unique-document.txt.pipedoc";
        record1.etag = "unique-etag";
        record1.sizeBytes = 512L;
        record1.contentType = "text/plain";
        record1.filename = "unique-document.txt";
        record1.createdAt = Instant.now();
        record1.nodeId = uuidGenerator.generateNodeId(record1.docId, record1.graphAddressId, record1.accountId);
        asserter.execute(() -> record1.persist());

        // Try to create record with same nodeId (PK) - should fail
        PipeDocRecord record2 = new PipeDocRecord();
        record2.nodeId = record1.nodeId; // Same nodeId (PK) as record1
        record2.docId = record1.docId; // Same docId, graphAddressId, accountId = same UUID
        record2.graphAddressId = record1.graphAddressId;
        record2.accountId = record1.accountId;
        record2.datasourceId = "ds-fail";
        record2.checksum = "different-checksum";
        record2.driveName = "different-drive";
        record2.objectKey = "uploads/different-document.txt";
        record2.pipedocObjectKey = "uploads/different-document.txt.pipedoc";
        record2.etag = "different-etag";
        record2.sizeBytes = 256L;
        record2.contentType = "text/plain";
        record2.filename = "different-document.txt";
        record2.createdAt = Instant.now();

        asserter.assertFailedWith(record2::persist, e -> {
            LOG.infof("Correctly caught unique constraint violation: %s", e.getMessage());
            assertThat(e.getMessage(), anyOf(
                    containsString("duplicate"),
                    containsString("unique"),
                    containsString("constraint"),
                    containsString("primary key"),
                    is(nullValue())
            ));
        });
    }

    @Test
    @RunOnVertxContext
    void testPipeDocRecordS3Reference(TransactionalUniAsserter asserter) {
        LOG.info("Testing PipeDocRecord S3 reference storage");

        // Create record with S3 reference
        PipeDocRecord record = new PipeDocRecord();
        record.docId = "s3-ref-doc-" + System.currentTimeMillis();
        record.graphAddressId = "node-s3-1";
        record.accountId = "acc-s3";
        record.datasourceId = "ds-s3";
        record.checksum = "sha256-abc123";
        record.driveName = "customer-drive";
        record.objectKey = "uploads/account-123/connector-456/doc-789/document.pdf";
        record.pipedocObjectKey = "uploads/account-123/connector-456/doc-789/document.pdf.pipedoc";
        record.versionId = "v2-abc-def-123";
        record.etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
        record.sizeBytes = 1048576L; // 1MB
        record.contentType = "application/pdf";
        record.filename = "document.pdf";
        record.createdAt = Instant.now();
        record.nodeId = uuidGenerator.generateNodeId(record.docId, record.graphAddressId, record.accountId);
        asserter.execute(() -> record.persist());

        asserter.assertThat(() -> PipeDocRecord.<PipeDocRecord>findById(record.nodeId), retrieved -> {
            assertThat("Should store object key", retrieved.objectKey,
                    is("uploads/account-123/connector-456/doc-789/document.pdf"));
            assertThat("Should store drive name", retrieved.driveName, is("customer-drive"));
            assertThat("Should store version id", retrieved.versionId, is("v2-abc-def-123"));
            assertThat("Should store etag", retrieved.etag, is("\"d41d8cd98f00b204e9800998ecf8427e\""));
        });
    }

    @Test
    @RunOnVertxContext
    void testPipeDocRecordQueries(TransactionalUniAsserter asserter) {
        LOG.info("Testing PipeDocRecord query operations");

        // Create multiple records
        PipeDocRecord record1 = new PipeDocRecord();
        record1.docId = "query-doc-1-" + System.currentTimeMillis();
        record1.graphAddressId = "node-q1";
        record1.accountId = "acc-q1";
        record1.datasourceId = "ds-q1";
        record1.checksum = "query123";
        record1.driveName = "query-drive-1";
        record1.objectKey = "uploads/query-document-1.txt";
        record1.pipedocObjectKey = "uploads/query-document-1.txt.pipedoc";
        record1.etag = "query-etag-1";
        record1.sizeBytes = 100L;
        record1.contentType = "text/plain";
        record1.filename = "query-document-1.txt";
        record1.createdAt = Instant.now();
        record1.nodeId = uuidGenerator.generateNodeId(record1.docId, record1.graphAddressId, record1.accountId);
        asserter.execute(() -> record1.persist());

        PipeDocRecord record2 = new PipeDocRecord();
        record2.docId = "query-doc-2-" + System.currentTimeMillis();
        record2.graphAddressId = "node-q2";
        record2.accountId = "acc-q2";
        record2.datasourceId = "ds-q2";
        record2.checksum = "query456";
        record2.driveName = "query-drive-1"; // Same drive as record1
        record2.objectKey = "uploads/query-document-2.txt";
        record2.pipedocObjectKey = "uploads/query-document-2.txt.pipedoc";
        record2.etag = "query-etag-2";
        record2.sizeBytes = 200L;
        record2.contentType = "text/plain";
        record2.filename = "query-document-2.txt";
        record2.createdAt = Instant.now();
        record2.nodeId = uuidGenerator.generateNodeId(record2.docId, record2.graphAddressId, record2.accountId);
        asserter.execute(() -> record2.persist());

        asserter.assertThat(() -> PipeDocRecord.count("driveName", "query-drive-1"), driveRecordsCount ->
                assertThat("Should find records for drive", driveRecordsCount, is(greaterThanOrEqualTo(2L))));

        asserter.assertThat(() -> PipeDocRecord.<PipeDocRecord>find("checksum", "query123").firstResult(), foundByChecksum -> {
            assertThat("Should find record by checksum", foundByChecksum, is(notNullValue()));
            assertThat("Found record should match", foundByChecksum.docId, is(record1.docId));
        });

        asserter.assertThat(() -> PipeDocRecord.count("sizeBytes > ?1", 150L), largeRecordsCount ->
                assertThat("Should find larger records", largeRecordsCount, is(greaterThanOrEqualTo(1L))));
    }
}