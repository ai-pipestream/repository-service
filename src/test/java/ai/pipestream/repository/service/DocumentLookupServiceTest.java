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
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for document lookup functionality in DocumentStorageService.
 * Tests FindDocumentById and FindDocumentsByCriteria methods.
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

        // Create test records with the same docId but different graph addresses
        PipeDocRecord record1 = createTestRecord(docId, "node-1", "acc-1", "ds-1", null);
        PipeDocRecord record2 = createTestRecord(docId, "node-2", "acc-1", "ds-1", "cluster-1");

        asserter.execute(() -> record1.persist());
        asserter.execute(() -> record2.persist());

        // Find by docId
        asserter.assertThat(() -> documentStorageService.findDocumentById(docId), results -> {
            assertThat("Should find documents", results, is(notNullValue()));
            assertThat("Should find at least 2 documents", results.size(), is(greaterThanOrEqualTo(2)));
            assertThat("All documents should have matching docId", 
                    results.stream().allMatch(r -> r.docId.equals(docId)), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_NotFound(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - not found case");

        String nonExistentDocId = "non-existent-doc-" + System.currentTimeMillis();

        // Find by non-existent docId
        asserter.assertThat(() -> documentStorageService.findDocumentById(nonExistentDocId), results -> {
            assertThat("Should return empty list for non-existent doc", results, is(notNullValue()));
            assertThat("Should return empty list", results.isEmpty(), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_NullDocId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - null docId");

        // Find by null docId should fail
        asserter.assertFailedWith(() -> documentStorageService.findDocumentById(null), 
                throwable -> {
                    assertThat("Should throw DocumentNotFoundException", 
                            throwable, instanceOf(DocumentStorageService.DocumentNotFoundException.class));
                    assertThat("Error message should mention null", 
                            throwable.getMessage(), containsString("null"));
                });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentById_BlankDocId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentById - blank docId");

        // Find by blank docId should fail
        asserter.assertFailedWith(() -> documentStorageService.findDocumentById("  "), 
                throwable -> {
                    assertThat("Should throw DocumentNotFoundException", 
                            throwable, instanceOf(DocumentStorageService.DocumentNotFoundException.class));
                    assertThat("Error message should mention blank", 
                            throwable.getMessage(), containsString("blank"));
                });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_ByDatasourceId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - by datasourceId");

        String uniqueDatasourceId = "ds-criteria-" + System.currentTimeMillis();
        String docId1 = "doc-ds-1-" + System.currentTimeMillis();
        String docId2 = "doc-ds-2-" + System.currentTimeMillis();

        // Create test records with the same datasourceId
        PipeDocRecord record1 = createTestRecord(docId1, "node-1", "acc-1", uniqueDatasourceId, null);
        PipeDocRecord record2 = createTestRecord(docId2, "node-2", "acc-1", uniqueDatasourceId, null);

        asserter.execute(() -> record1.persist());
        asserter.execute(() -> record2.persist());

        // Search by datasourceId
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(uniqueDatasourceId, null, null, null, null, null, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 2 documents", result.documents().size(), is(greaterThanOrEqualTo(2)));
            assertThat("Total count should be at least 2", result.totalCount(), is(greaterThanOrEqualTo(2L)));
            assertThat("All documents should have matching datasourceId",
                    result.documents().stream().allMatch(r -> r.datasourceId.equals(uniqueDatasourceId)), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_ByAccountId(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - by accountId");

        String uniqueAccountId = "acc-criteria-" + System.currentTimeMillis();
        String docId = "doc-acc-" + System.currentTimeMillis();

        // Create test record
        PipeDocRecord record = createTestRecord(docId, "node-1", uniqueAccountId, "ds-1", null);
        asserter.execute(() -> record.persist());

        // Search by accountId
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, null, null, null, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
            assertThat("All documents should have matching accountId",
                    result.documents().stream().allMatch(r -> r.accountId.equals(uniqueAccountId)), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_ByClusterId_Intake(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - by clusterId (intake documents)");

        String docId = "doc-intake-" + System.currentTimeMillis();
        String uniqueAccountId = "acc-intake-" + System.currentTimeMillis();

        // Create intake document (clusterId = null)
        PipeDocRecord intakeRecord = createTestRecord(docId, "node-1", uniqueAccountId, "ds-1", null);
        asserter.execute(() -> intakeRecord.persist());

        // Search for intake documents (clusterId = "")
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, "", null, null, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
            assertThat("All documents should have null clusterId",
                    result.documents().stream().allMatch(r -> r.clusterId == null), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_ByClusterId_Processed(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - by clusterId (processed documents)");

        String uniqueClusterId = "cluster-test-" + System.currentTimeMillis();
        String docId = "doc-processed-" + System.currentTimeMillis();
        String uniqueAccountId = "acc-processed-" + System.currentTimeMillis();

        // Create processed document
        PipeDocRecord processedRecord = createTestRecord(docId, "node-1", uniqueAccountId, "ds-1", uniqueClusterId);
        asserter.execute(() -> processedRecord.persist());

        // Search by specific clusterId
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, uniqueClusterId, null, null, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
            assertThat("All documents should have matching clusterId",
                    result.documents().stream().allMatch(r -> uniqueClusterId.equals(r.clusterId)), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_ByDateRange(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - by date range");

        Instant now = Instant.now();
        Instant yesterday = now.minus(1, ChronoUnit.DAYS);
        Instant tomorrow = now.plus(1, ChronoUnit.DAYS);

        String uniqueAccountId = "acc-date-" + System.currentTimeMillis();
        String docId = "doc-date-" + System.currentTimeMillis();

        // Create record with current timestamp
        PipeDocRecord record = createTestRecord(docId, "node-1", uniqueAccountId, "ds-1", null);
        record.createdAt = now;
        asserter.execute(() -> record.persist());

        // Search with date range that includes today
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, null, yesterday, tomorrow, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
            assertThat("All documents should be within date range",
                    result.documents().stream().allMatch(r -> 
                            !r.createdAt.isBefore(yesterday) && !r.createdAt.isAfter(tomorrow)), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_DateRange_NoResults(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - date range with no results");

        Instant longAgo = Instant.now().minus(365, ChronoUnit.DAYS);
        Instant stillLongAgo = longAgo.plus(1, ChronoUnit.DAYS);

        String uniqueAccountId = "acc-nodate-" + System.currentTimeMillis();

        // Search with date range in the past
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, null, longAgo, stillLongAgo, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find no documents", result.documents().isEmpty(), is(true));
            assertThat("Total count should be 0", result.totalCount(), is(0L));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_Pagination(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - pagination");

        String uniqueDatasourceId = "ds-page-" + System.currentTimeMillis();

        // Create multiple test records
        for (int i = 0; i < 5; i++) {
            String docId = "doc-page-" + i + "-" + System.currentTimeMillis();
            PipeDocRecord record = createTestRecord(docId, "node-" + i, "acc-1", uniqueDatasourceId, null);
            asserter.execute(() -> record.persist());
        }

        // Search with page size of 2
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(uniqueDatasourceId, null, null, null, null, null, 1, 2);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should return 2 documents on first page", result.documents().size(), is(2));
            assertThat("Total count should be at least 5", result.totalCount(), is(greaterThanOrEqualTo(5L)));
            assertThat("Page size should be 2", result.pageSize(), is(2));
            assertThat("Current page should be 1", result.currentPage(), is(1));
            assertThat("Total pages should be at least 3", result.totalPages(), is(greaterThanOrEqualTo(3)));
        });

        // Get second page
        DocumentStorageService.DocumentSearchCriteria criteria2 = 
                new DocumentStorageService.DocumentSearchCriteria(uniqueDatasourceId, null, null, null, null, null, 2, 2);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria2), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should return 2 documents on second page", result.documents().size(), is(2));
            assertThat("Current page should be 2", result.currentPage(), is(2));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_CombinedFilters(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - combined filters");

        String uniqueAccountId = "acc-combined-" + System.currentTimeMillis();
        String uniqueDatasourceId = "ds-combined-" + System.currentTimeMillis();
        String uniqueConnectorId = "conn-combined-" + System.currentTimeMillis();
        String docId = "doc-combined-" + System.currentTimeMillis();

        Instant now = Instant.now();

        // Create test record
        PipeDocRecord record = createTestRecord(docId, "node-1", uniqueAccountId, uniqueDatasourceId, null);
        record.connectorId = uniqueConnectorId;
        record.createdAt = now;
        asserter.execute(() -> record.persist());

        // Search with multiple filters
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(
                        uniqueDatasourceId, 
                        uniqueAccountId, 
                        uniqueConnectorId, 
                        "", 
                        now.minus(1, ChronoUnit.HOURS), 
                        now.plus(1, ChronoUnit.HOURS), 
                        1, 
                        20
                );

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
            assertThat("All documents should match all criteria",
                    result.documents().stream().allMatch(r -> 
                            r.accountId.equals(uniqueAccountId) &&
                            r.datasourceId.equals(uniqueDatasourceId) &&
                            r.connectorId.equals(uniqueConnectorId) &&
                            r.clusterId == null
                    ), is(true));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_NullCriteria(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - null criteria");

        // Search with null criteria should fail
        asserter.assertFailedWith(() -> documentStorageService.findDocumentsByCriteria(null), 
                throwable -> {
                    assertThat("Should throw IllegalArgumentException", 
                            throwable, instanceOf(IllegalArgumentException.class));
                    assertThat("Error message should mention null", 
                            throwable.getMessage(), containsString("null"));
                });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_InvalidPageSize(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - invalid page size");

        // Create criteria with page size > 1000 should fail during construction
        asserter.assertFailedWith(() -> {
            new DocumentStorageService.DocumentSearchCriteria(null, null, null, null, null, null, 1, 1001);
            return null;
        }, throwable -> {
            assertThat("Should throw IllegalArgumentException", 
                    throwable, instanceOf(IllegalArgumentException.class));
            assertThat("Error message should mention page size", 
                    throwable.getMessage(), containsString("Page size"));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindDocumentsByCriteria_NoCriteria_ReturnsAll(TransactionalUniAsserter asserter) {
        LOG.info("Testing findDocumentsByCriteria - no criteria returns all");

        // Create a unique account to isolate results
        String uniqueAccountId = "acc-all-" + System.currentTimeMillis();
        String docId = "doc-all-" + System.currentTimeMillis();

        PipeDocRecord record = createTestRecord(docId, "node-1", uniqueAccountId, "ds-1", null);
        asserter.execute(() -> record.persist());

        // Search with no filters (should return all for this account)
        DocumentStorageService.DocumentSearchCriteria criteria = 
                new DocumentStorageService.DocumentSearchCriteria(null, uniqueAccountId, null, null, null, null, 1, 20);

        asserter.assertThat(() -> documentStorageService.findDocumentsByCriteria(criteria), result -> {
            assertThat("Should return search result", result, is(notNullValue()));
            assertThat("Should find at least 1 document", result.documents().size(), is(greaterThanOrEqualTo(1)));
        });
    }

    /**
     * Helper method to create a test PipeDocRecord.
     */
    private PipeDocRecord createTestRecord(String docId, String graphAddressId, String accountId, 
                                          String datasourceId, String clusterId) {
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
        record.createdAt = Instant.now();
        record.nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        return record;
    }
}
