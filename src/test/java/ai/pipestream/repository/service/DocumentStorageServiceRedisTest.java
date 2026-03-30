package ai.pipestream.repository.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.account.AccountCacheService;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DocumentStorageService} focusing on the Redis hot cache path.
 * <p>
 * Since the test profile defaults to {@code s3-only} mode, these tests verify:
 * <ul>
 *   <li>S3-only mode stores work correctly (baseline)</li>
 *   <li>Redis cache can be used for direct get/put independently</li>
 *   <li>The get() method correctly handles cache miss and S3 fallback</li>
 * </ul>
 */
@QuarkusTest
@QuarkusTestResource(S3TestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
public class DocumentStorageServiceRedisTest {

    private static final Logger LOG = Logger.getLogger(DocumentStorageServiceRedisTest.class);
    private static final String VALID_ACCOUNT = "valid-account";
    private static final String DATASOURCE = "ds-redis-test";
    private static final String CONNECTOR = "conn-redis-test";

    @Inject DocumentStorageService storageService;
    @Inject RedisDocumentCache redisCache;
    @Inject RedisStorageConfig redisConfig;
    @Inject PipeDocUuidGenerator uuidGenerator;
    @Inject AccountCacheService accountCacheService;

    @BeforeEach
    void resetAccountCache() {
        accountCacheService.resetCache().await().indefinitely();
    }

    @Test
    @RunOnVertxContext
    void store_s3OnlyMode_createsRecordWithAvailableStatus(TransactionalUniAsserter asserter) {
        String docId = "s3only-doc-" + System.currentTimeMillis();
        PipeDoc doc = buildTestDoc(docId);

        asserter.assertThat(
                () -> storageService.store(doc, "req-" + docId),
                stored -> {
                    assertThat(stored)
                            .as("store() should return a StoredDocument")
                            .isNotNull();
                    assertThat(stored.documentId())
                            .as("StoredDocument should have a non-blank documentId (nodeId)")
                            .isNotBlank();
                    assertThat(stored.s3Key())
                            .as("S3 key should be set")
                            .isNotBlank();
                    assertThat(stored.sizeBytes())
                            .as("Size should be positive")
                            .isGreaterThan(0);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void store_s3OnlyMode_dbRecordHasAvailableStatus(TransactionalUniAsserter asserter) {
        String docId = "s3status-doc-" + System.currentTimeMillis();
        PipeDoc doc = buildTestDoc(docId);

        asserter.assertThat(
                () -> storageService.store(doc, "req-" + docId)
                        .flatMap(stored -> PipeDocRecord.<PipeDocRecord>findById(UUID.fromString(stored.documentId()))),
                record -> {
                    assertThat(record)
                            .as("PipeDocRecord should exist in DB")
                            .isNotNull();
                    assertThat(record.status)
                            .as("Status should be AVAILABLE in s3-only mode (synchronous write)")
                            .isEqualTo(BackgroundS3Flusher.STATUS_AVAILABLE);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void storeAndGet_s3Only_roundTrips(TransactionalUniAsserter asserter) {
        String docId = "roundtrip-doc-" + System.currentTimeMillis();
        PipeDoc doc = buildTestDoc(docId);

        asserter.assertThat(
                () -> storageService.store(doc, "req-" + docId)
                        .flatMap(stored -> storageService.get(stored.documentId())),
                retrieved -> {
                    assertThat(retrieved)
                            .as("get() should return the stored PipeDoc")
                            .isNotNull();
                    assertThat(retrieved.getDocId())
                            .as("Retrieved doc should have the same docId")
                            .isEqualTo(docId);
                    assertThat(retrieved.getOwnership().getAccountId())
                            .as("Retrieved doc should preserve ownership accountId")
                            .isEqualTo(VALID_ACCOUNT);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void get_nonexistentNodeId_returnsNull(TransactionalUniAsserter asserter) {
        String fakeNodeId = UUID.randomUUID().toString();

        asserter.assertThat(
                () -> storageService.get(fakeNodeId),
                result -> assertThat(result)
                        .as("get() for nonexistent nodeId should return null")
                        .isNull()
        );
    }

    @Test
    @RunOnVertxContext
    void get_invalidUuid_returnsNull(UniAsserter asserter) {
        asserter.assertThat(
                () -> storageService.get("not-a-uuid"),
                result -> assertThat(result)
                        .as("get() with invalid UUID should return null gracefully")
                        .isNull()
        );
    }

    @Test
    @RunOnVertxContext
    void get_nullNodeId_returnsNull(UniAsserter asserter) {
        asserter.assertThat(
                () -> storageService.get(null),
                result -> assertThat(result)
                        .as("get() with null nodeId should return null")
                        .isNull()
        );
    }

    @Test
    @RunOnVertxContext
    void redisCache_inS3OnlyMode_isNoOp(UniAsserter asserter) {
        String nodeId = UUID.randomUUID().toString();
        byte[] payload = "test-bytes".getBytes();

        // In s3-only mode, put is a no-op and get returns null
        asserter.execute(() -> redisCache.put(nodeId, payload));
        asserter.assertThat(
                () -> redisCache.get(nodeId),
                bytes -> assertThat(bytes)
                        .as("Redis cache should be a no-op in s3-only mode (returns null)")
                        .isNull()
        );
    }

    @Test
    void redisCache_inS3OnlyMode_isDisabled() {
        assertThat(redisCache.isEnabled())
                .as("Redis cache should report disabled in s3-only mode")
                .isFalse();
    }

    @Test
    @RunOnVertxContext
    void getByCompositeKey_s3Only_roundTrips(TransactionalUniAsserter asserter) {
        String docId = "composite-doc-" + System.currentTimeMillis();
        PipeDoc doc = buildTestDoc(docId);

        asserter.assertThat(
                () -> storageService.store(doc, "req-" + docId)
                        .flatMap(stored -> storageService.getByCompositeKey(docId, DATASOURCE, VALID_ACCOUNT)),
                retrieved -> {
                    assertThat(retrieved)
                            .as("getByCompositeKey() should return the stored PipeDoc")
                            .isNotNull();
                    assertThat(retrieved.getDocId())
                            .as("Retrieved doc should match the stored docId")
                            .isEqualTo(docId);
                }
        );
    }

    @Test
    void storageMode_inTestProfile_isS3Only() {
        assertThat(redisConfig.resolvedStorageMode())
                .as("Test profile should default to S3_ONLY mode")
                .isEqualTo(StorageMode.S3_ONLY);
    }

    @Test
    @RunOnVertxContext
    void store_nullDocument_fails(UniAsserter asserter) {
        asserter.assertFailedWith(
                () -> storageService.store(null),
                e -> assertThat(e)
                        .as("store(null) should fail with IllegalArgumentException")
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("null")
        );
    }

    @Test
    @RunOnVertxContext
    void store_missingOwnership_fails(UniAsserter asserter) {
        PipeDoc noOwnership = PipeDoc.newBuilder()
                .setDocId("no-ownership-" + System.currentTimeMillis())
                .build();

        asserter.assertFailedWith(
                () -> storageService.store(noOwnership),
                e -> assertThat(e)
                        .as("store() without ownership should fail")
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("ownership")
        );
    }

    private PipeDoc buildTestDoc(String docId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(VALID_ACCOUNT)
                        .setDatasourceId(DATASOURCE)
                        .setConnectorId(CONNECTOR)
                        .build())
                .build();
    }
}
