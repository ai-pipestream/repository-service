package ai.pipestream.repository.service;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RedisDocumentCache}.
 * Uses Quarkus DevServices to auto-provision a Redis container.
 */
@QuarkusTest
public class RedisDocumentCacheTest {

    private static final Logger LOG = Logger.getLogger(RedisDocumentCacheTest.class);

    @Inject
    RedisDocumentCache redisCache;

    @Test
    @RunOnVertxContext
    void putAndGet_roundTripsBytes(UniAsserter asserter) {
        String nodeId = UUID.randomUUID().toString();
        byte[] payload = "hello-redis-test".getBytes(StandardCharsets.UTF_8);

        asserter.execute(() -> redisCache.put(nodeId, payload));
        asserter.assertThat(
                () -> redisCache.get(nodeId),
                bytes -> assertThat(bytes)
                        .as("GET after PUT should return the same bytes")
                        .isEqualTo(payload)
        );
    }

    @Test
    @RunOnVertxContext
    void get_cacheMiss_returnsNull(UniAsserter asserter) {
        String missingId = "nonexistent-" + UUID.randomUUID();

        asserter.assertThat(
                () -> redisCache.get(missingId),
                bytes -> assertThat(bytes)
                        .as("GET for a key that was never SET should return null")
                        .isNull()
        );
    }

    @Test
    @RunOnVertxContext
    void putAndDelete_removesKey(UniAsserter asserter) {
        String nodeId = UUID.randomUUID().toString();
        byte[] payload = "to-be-deleted".getBytes(StandardCharsets.UTF_8);

        asserter.execute(() -> redisCache.put(nodeId, payload));
        asserter.execute(() -> redisCache.delete(nodeId));
        asserter.assertThat(
                () -> redisCache.get(nodeId),
                bytes -> assertThat(bytes)
                        .as("GET after DELETE should return null")
                        .isNull()
        );
    }

    @Test
    @RunOnVertxContext
    void put_overwritesExistingValue(UniAsserter asserter) {
        String nodeId = UUID.randomUUID().toString();
        byte[] v1 = "version-1".getBytes(StandardCharsets.UTF_8);
        byte[] v2 = "version-2-updated".getBytes(StandardCharsets.UTF_8);

        asserter.execute(() -> redisCache.put(nodeId, v1));
        asserter.execute(() -> redisCache.put(nodeId, v2));
        asserter.assertThat(
                () -> redisCache.get(nodeId),
                bytes -> assertThat(bytes)
                        .as("Second PUT should overwrite the first value")
                        .isEqualTo(v2)
        );
    }

    @Test
    @RunOnVertxContext
    void put_largePayload_roundTrips(UniAsserter asserter) {
        String nodeId = UUID.randomUUID().toString();
        // 1MB payload simulating a real PipeDoc protobuf
        byte[] largePayload = new byte[1024 * 1024];
        for (int i = 0; i < largePayload.length; i++) {
            largePayload[i] = (byte) (i % 256);
        }

        asserter.execute(() -> redisCache.put(nodeId, largePayload));
        asserter.assertThat(
                () -> redisCache.get(nodeId),
                bytes -> {
                    assertThat(bytes)
                            .as("Large payload should round-trip through Redis intact")
                            .hasSize(largePayload.length);
                    assertThat(bytes[0]).as("First byte").isEqualTo((byte) 0);
                    assertThat(bytes[255]).as("Byte at index 255").isEqualTo((byte) 255);
                    assertThat(bytes[1024]).as("Byte at index 1024").isEqualTo((byte) 0);
                }
        );
    }

    @Test
    @RunOnVertxContext
    void multipleKeys_isolated(UniAsserter asserter) {
        String nodeA = UUID.randomUUID().toString();
        String nodeB = UUID.randomUUID().toString();
        byte[] payloadA = "payload-A".getBytes(StandardCharsets.UTF_8);
        byte[] payloadB = "payload-B".getBytes(StandardCharsets.UTF_8);

        asserter.execute(() -> redisCache.put(nodeA, payloadA));
        asserter.execute(() -> redisCache.put(nodeB, payloadB));
        asserter.assertThat(
                () -> redisCache.get(nodeA),
                bytes -> assertThat(bytes)
                        .as("Key A should return payload A, not B")
                        .isEqualTo(payloadA)
        );
        asserter.assertThat(
                () -> redisCache.get(nodeB),
                bytes -> assertThat(bytes)
                        .as("Key B should return payload B, not A")
                        .isEqualTo(payloadB)
        );
    }

    @Test
    @RunOnVertxContext
    void delete_nonexistentKey_doesNotThrow(UniAsserter asserter) {
        String missingId = "never-existed-" + UUID.randomUUID();
        // Should complete without error
        asserter.execute(() -> redisCache.delete(missingId));
    }
}
