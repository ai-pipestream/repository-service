package ai.pipestream.repository.service;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.vertx.RunOnVertxContext;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for document lookup functionality in DocumentStorageService.
 */
@QuarkusTest
public class DocumentLookupServiceTest {

    private static final Logger LOG = Logger.getLogger(DocumentLookupServiceTest.class);

    @Inject
    DocumentStorageService documentStorageService;

    @Inject
    PipeDocUuidGenerator uuidGenerator;

    @Test
    @RunOnVertxContext
    void testFindDocumentById_Success(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - success case");
        String docId = "test-lookup-doc-" + System.currentTimeMillis();
        PipeDocRecord record1 = createTestRecord(docId, "node-1", "acc-1", "ds-1", null);
        PipeDocRecord record2 = createTestRecord(docId, "node-2", "acc-1", "ds-1", "cluster-1");
        asserter.execute(() -> record1.persist());
        asserter.execute(() -> record2.persist());
        asserter.assertThat(() -> documentStorageService.findDocumentById(docId), results -> {
            assertThat(results, is(notNullValue()));
            assertThat(results.size(), is(greaterThanOrEqualTo(2)));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_NotFound(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - not found case");
        String nonExistentDocId = "non-existent-doc-" + System.currentTimeMillis();
        asserter.assertThat(() -> documentStorageService.findDocumentById(nonExistentDocId), results -> {
            assertThat(results, is(notNullValue()));
            assertThat(results.isEmpty(), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_NullDocId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - null docId");
        asserter.assertFailedWith(() -> documentStorageService.findDocumentById(null), 
                throwable -> assertThat(throwable, instanceOf(DocumentStorageService.DocumentNotFoundException.class)));
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_BlankDocId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - blank docId");
        asserter.assertFailedWith(() -> documentStorageService.findDocumentById("  "), 
                throwable -> assertThat(throwable, instanceOf(DocumentStorageService.DocumentNotFoundException.class)));
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_Pagination(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - pagination");
        String uniqueDatasourceId = "ds-page-" + System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            String docId = "doc-page-" + i + "-" + System.currentTimeMillis();
            PipeDocRecord record = createTestRecord(docId, "node-" + i, "acc-1", uniqueDatasourceId, null);
            asserter.execute(() -> record.persist());
        }
        DocumentStorageService.DocumentSearchCriteria criteria = new DocumentStorageService.DocumentSearchCriteria(uniqueDatasourceId, null, null, null, null, null, 1, 2);
        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat(result.documents().size(), is(2));
            assertThat(result.totalCount(), is(greaterThanOrEqualTo(5L)));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_NullCriteria(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - null criteria");
        asserter.assertFailedWith(() -> documentStorageService.findDocumentsByCriteria(null), 
                throwable -> assertThat(throwable, instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_InvalidPageSize(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - invalid page size");
        try {
            new DocumentStorageService.DocumentSearchCriteria(null, null, null, null, null, null, 1, 1001);
            org.junit.jupiter.api.Assertions.fail("Should throw");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Invalid pagination"));
        }
    }

    private PipeDocRecord createTestRecord(String docId, String graphAddressId, String accountId, String datasourceId, String clusterId) {
        PipeDocRecord record = new PipeDocRecord();
        record.docId = docId;
        record.graphAddressId = graphAddressId;
        record.accountId = accountId;
        record.datasourceId = datasourceId;
        record.clusterId = clusterId;
        record.connectorId = "test-connector";
        record.checksum = "test-checksum-" + System.currentTimeMillis();
        record.driveName = "test-drive";
        record.objectKey = "test/path/" + docId;
        record.pipedocObjectKey = "test/path/" + docId + ".pb";
        record.versionId = "v1.0";
        record.etag = "etag-" + System.currentTimeMillis();
        record.sizeBytes = 1024L;
        record.contentType = "application/x-protobuf";
        record.filename = docId + ".pb";
        record.acls = new ArrayList<>();
        record.acls.add("user:test");
        record.createdAt = Instant.now();
        record.nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        return record;
    }
}
