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
 * Real Hibernate CRUD tests for PipeDocRecord entity (no Mockito).
 * Updated for Reactive Panache.
 */
@QuarkusTest
public class PipeDocRecordEntityTest {

    private static final Logger LOG = Logger.getLogger(PipeDocRecordEntityTest.class);

    @Test
    void testCreateAndFindPipeDocRecord() {
        LOG.info("Testing PipeDocRecord entity CRUD operations");

        // Create a new PipeDocRecord (metadata only, no pipedoc bytes in DB)
        PipeDocRecord record = new PipeDocRecord();
        record.docId = "test-doc-" + System.currentTimeMillis();
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

        // Persist the record
        record::persist).await().indefinitely();;

        // Verify it was saved
        assertThat(record.id, is(notNullValue()));

        LOG.infof("Created PipeDocRecord: id=%d, docId=%s", record.id, record.docId);

        // Test findById
        PipeDocRecord foundById = PipeDocRecord.<PipeDocRecord>findById(record.id).await().indefinitely();
        assertThat(foundById, is(notNullValue()));
        assertThat(foundById.docId, is(record.docId));
        assertThat(foundById.checksum, is("checksum123"));
        assertThat(foundById.sizeBytes, is(1024L));

        // Test find by docId
        PipeDocRecord foundByDocId = PipeDocRecord.<PipeDocRecord>find("docId", record.docId).firstResult().await().indefinitely();
        assertThat(foundByDocId, is(notNullValue()));
        assertThat(foundByDocId.id, is(record.id));

        LOG.infof("All PipeDocRecord lookups working correctly");
    }

    @Test
    void testPipeDocRecordUniqueConstraints() {
        LOG.info("Testing PipeDocRecord unique constraints");

        // Create first record
        PipeDocRecord record1 = new PipeDocRecord();
        record1.docId = "unique-doc-" + System.currentTimeMillis();
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
        record1::persist).await().indefinitely();;

        // Try to create record with same docId - should fail
        PipeDocRecord record2 = new PipeDocRecord();
        record2.docId = record1.docId; // Same docId as record1
        record2.accountId = "acc-fail";
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

        try {
            record2::persist).await().indefinitely();;
            assertThat("Should have failed due to unique constraint", false);
        } catch (Exception e) {
            LOG.infof("Correctly caught unique constraint violation: %s", e.getMessage());
            // In reactive, the exception might be wrapped or different
            assertThat(e.getMessage(), anyOf(
                containsString("duplicate"),
                containsString("unique"),
                containsString("constraint"),
                is(nullValue()) // Sometimes the error is only on flush/commit
            ));
        }
    }

    @Test
    void testPipeDocRecordS3Reference() {
        LOG.info("Testing PipeDocRecord S3 reference storage");

        // Create record with S3 reference
        PipeDocRecord record = new PipeDocRecord();
        record.docId = "s3-ref-doc-" + System.currentTimeMillis();
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
        record::persist).await().indefinitely();;

        // Verify S3 reference is stored correctly
        PipeDocRecord retrieved = PipeDocRecord.<PipeDocRecord>findById(record.id).await().indefinitely();
        assertThat("Should store object key", retrieved.objectKey,
            is("uploads/account-123/connector-456/doc-789/document.pdf"));
        assertThat("Should store drive name", retrieved.driveName, is("customer-drive"));
        assertThat("Should store version id", retrieved.versionId, is("v2-abc-def-123"));
        assertThat("Should store etag", retrieved.etag, is("\"d41d8cd98f00b204e9800998ecf8427e\""));

        LOG.infof("PipeDocRecord S3 reference storage working correctly");
    }

    @Test
    void testPipeDocRecordQueries() {
        LOG.info("Testing PipeDocRecord query operations");

        // Create multiple records
        PipeDocRecord record1 = new PipeDocRecord();
        record1.docId = "query-doc-1-" + System.currentTimeMillis();
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
        record1::persist).await().indefinitely();;

        PipeDocRecord record2 = new PipeDocRecord();
        record2.docId = "query-doc-2-" + System.currentTimeMillis();
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
        record2::persist).await().indefinitely();;

        // Test find by driveName
        long driveRecordsCount = PipeDocRecord.count("driveName", "query-drive-1").await().indefinitely();
        assertThat("Should find records for drive", driveRecordsCount, is(greaterThanOrEqualTo(2L)));

        // Test find by checksum
        PipeDocRecord foundByChecksum = PipeDocRecord.<PipeDocRecord>find("checksum", "query123").firstResult().await().indefinitely();
        assertThat("Should find record by checksum", foundByChecksum, is(notNullValue()));
        assertThat("Found record should match", foundByChecksum.docId, is(record1.docId));

        // Test size-based queries
        long largeRecordsCount = PipeDocRecord.count("sizeBytes > ?1", 150L).await().indefinitely();
        assertThat("Should find larger records", largeRecordsCount, is(greaterThanOrEqualTo(1L)));

        LOG.infof("PipeDocRecord queries working correctly");
    }
}