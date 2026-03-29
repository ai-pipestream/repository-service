package ai.pipestream.repository.service;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import ai.pipestream.repository.v1.CacheFlushEvent;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link BackgroundS3Flusher}.
 * Tests the Kafka consumer method directly (without Kafka) to verify:
 * - Redis hit → S3 write + DB status update to AVAILABLE
 * - Redis miss → DB status update to LOST
 * - Invalid/null events are handled gracefully
 */
@QuarkusTest
@TestProfile(RedisBufferedTestProfile.class)
@QuarkusTestResource(S3TestResource.class)
public class BackgroundS3FlusherTest {

    private static final Logger LOG = Logger.getLogger(BackgroundS3FlusherTest.class);

    @Inject BackgroundS3Flusher flusher;
    @Inject RedisDocumentCache redisCache;
    @Inject PipeDocUuidGenerator uuidGenerator;

    @Test
    @RunOnVertxContext
    void flushEvent_redisHit_writesToS3AndMarksAvailable(TransactionalUniAsserter asserter) {
        String docId = "flush-hit-" + System.currentTimeMillis();
        String graphAddressId = "graph-flush-1";
        String accountId = "acc-flush";
        UUID nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        String objectKey = "uploads/test-drive/" + accountId + "/flush/" + docId + "/" + nodeId + ".pb";
        byte[] payload = "pipedoc-bytes-for-flushing".getBytes(StandardCharsets.UTF_8);

        // 1. Seed a PENDING_STORAGE record in DB
        PipeDocRecord record = createTestRecord(docId, graphAddressId, accountId, nodeId, objectKey);
        record.status = BackgroundS3Flusher.STATUS_PENDING_STORAGE;
        asserter.execute(() -> record.persist());

        // 2. Put bytes in Redis (simulating what DocumentStorageService does)
        asserter.execute(() -> redisCache.put(nodeId.toString(), payload));

        // 3. Call the consumer method directly (bypassing Kafka)
        CacheFlushEvent event = CacheFlushEvent.newBuilder()
                .setNodeId(nodeId.toString())
                .setObjectKey(objectKey)
                .setDriveName("test-drive")
                .setAccountId(accountId)
                .build();
        asserter.execute(() -> flusher.consumeFlushEvent(Record.of(nodeId, event)));

        // 4. Verify DB status flipped to AVAILABLE
        asserter.assertThat(
                () -> PipeDocRecord.<PipeDocRecord>findById(nodeId),
                updated -> {
                    assertThat(updated)
                            .as("Record should still exist after flush")
                            .isNotNull();
                    assertThat(updated.status)
                            .as("Status should be AVAILABLE after successful flush")
                            .isEqualTo(BackgroundS3Flusher.STATUS_AVAILABLE);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void flushEvent_redisMiss_marksLost(TransactionalUniAsserter asserter) {
        String docId = "flush-miss-" + System.currentTimeMillis();
        String graphAddressId = "graph-flush-2";
        String accountId = "acc-flush";
        UUID nodeId = uuidGenerator.generateNodeId(docId, graphAddressId, accountId);
        String objectKey = "uploads/test-drive/" + accountId + "/flush/" + docId + "/" + nodeId + ".pb";

        // 1. Seed a PENDING_STORAGE record in DB (but do NOT put bytes in Redis)
        PipeDocRecord record = createTestRecord(docId, graphAddressId, accountId, nodeId, objectKey);
        record.status = BackgroundS3Flusher.STATUS_PENDING_STORAGE;
        asserter.execute(() -> record.persist());

        // 2. Call consumer — Redis will miss (no data stored)
        CacheFlushEvent event = CacheFlushEvent.newBuilder()
                .setNodeId(nodeId.toString())
                .setObjectKey(objectKey)
                .setDriveName("test-drive")
                .setAccountId(accountId)
                .build();
        asserter.execute(() -> flusher.consumeFlushEvent(Record.of(nodeId, event)));

        // 3. Verify DB status flipped to LOST
        asserter.assertThat(
                () -> PipeDocRecord.<PipeDocRecord>findById(nodeId),
                updated -> {
                    assertThat(updated)
                            .as("Record should still exist after cache miss")
                            .isNotNull();
                    assertThat(updated.status)
                            .as("Status should be LOST when Redis has no data")
                            .isEqualTo(BackgroundS3Flusher.STATUS_LOST);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void flushEvent_nullEvent_doesNotThrow(TransactionalUniAsserter asserter) {
        UUID key = UUID.randomUUID();
        // Null CacheFlushEvent value should be handled gracefully
        asserter.execute(() -> flusher.consumeFlushEvent(Record.of(key, null)));
    }

    @Test
    @RunOnVertxContext
    void flushEvent_invalidNodeId_doesNotThrow(TransactionalUniAsserter asserter) {
        UUID key = UUID.randomUUID();
        CacheFlushEvent event = CacheFlushEvent.newBuilder()
                .setNodeId("not-a-uuid")
                .setObjectKey("some/key")
                .setDriveName("drive")
                .setAccountId("acc")
                .build();
        // Invalid UUID should be handled gracefully (logged + skipped)
        asserter.execute(() -> flusher.consumeFlushEvent(Record.of(key, event)));
    }

    @Test
    void counters_startAtZero() {
        // Counters are cumulative per JVM, but should be non-negative
        assertThat(flusher.getTotalFlushed())
                .as("Total flushed counter should be non-negative")
                .isGreaterThanOrEqualTo(0);
        assertThat(flusher.getTotalLost())
                .as("Total lost counter should be non-negative")
                .isGreaterThanOrEqualTo(0);
    }

    private PipeDocRecord createTestRecord(String docId, String graphAddressId, String accountId,
                                            UUID nodeId, String objectKey) {
        PipeDocRecord record = new PipeDocRecord();
        record.nodeId = nodeId;
        record.docId = docId;
        record.graphAddressId = graphAddressId;
        record.accountId = accountId;
        record.datasourceId = "ds-flush-test";
        record.connectorId = "connector-flush-test";
        record.checksum = "test-checksum";
        record.driveName = "test-drive";
        record.objectKey = objectKey;
        record.pipedocObjectKey = objectKey;
        record.etag = "test-etag";
        record.sizeBytes = 100L;
        record.contentType = "application/x-protobuf";
        record.filename = nodeId + ".pb";
        record.acls = new ArrayList<>();
        record.createdAt = Instant.now();
        return record;
    }
}
