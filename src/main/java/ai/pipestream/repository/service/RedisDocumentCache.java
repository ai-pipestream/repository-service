package ai.pipestream.repository.service;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Thin cache layer over Redis for PipeDoc protobuf bytes.
 * <p>
 * Key format: {@code pipedoc:{nodeId}} where nodeId is the deterministic UUID
 * generated from (docId, graphAddressId, accountId).
 * <p>
 * Values are raw protobuf bytes — no serialization overhead beyond what
 * PipeDoc.toByteArray() already does.
 */
@ApplicationScoped
public class RedisDocumentCache {

    private static final Logger LOG = Logger.getLogger(RedisDocumentCache.class);
    private static final String KEY_PREFIX = "pipedoc:";

    @Inject
    ReactiveRedisDataSource redis;

    @Inject
    RedisStorageConfig config;

    private ReactiveValueCommands<String, byte[]> commands;

    @PostConstruct
    void init() {
        commands = redis.value(byte[].class);
        LOG.infof("RedisDocumentCache initialized (ttl=%dh, refreshOnRead=%s)",
                config.redis().ttlHours(), config.redis().refreshTtlOnRead());
    }

    /**
     * Store a document's protobuf bytes in Redis with TTL.
     *
     * @param nodeId the deterministic UUID for this document version
     * @param pipeDocBytes serialized PipeDoc protobuf
     * @return Uni completing when the write is acknowledged
     */
    public Uni<Void> put(String nodeId, byte[] pipeDocBytes) {
        String key = KEY_PREFIX + nodeId;
        Duration ttl = Duration.ofHours(config.redis().ttlHours());
        return commands.setex(key, ttl.toSeconds(), pipeDocBytes)
                .invoke(() -> LOG.debugf("Redis PUT %s (%d bytes, ttl=%s)", key, pipeDocBytes.length, ttl))
                .replaceWithVoid();
    }

    /**
     * Retrieve a document's protobuf bytes from Redis.
     * Optionally refreshes the TTL on hit to keep hot documents alive.
     *
     * @param nodeId the deterministic UUID
     * @return the protobuf bytes, or null on cache miss
     */
    public Uni<byte[]> get(String nodeId) {
        String key = KEY_PREFIX + nodeId;
        return commands.get(key)
                .onItem().ifNotNull().call(bytes -> {
                    if (config.redis().refreshTtlOnRead()) {
                        Duration ttl = Duration.ofHours(config.redis().ttlHours());
                        return redis.key().expire(key, ttl.toSeconds())
                                .replaceWithVoid();
                    }
                    return Uni.createFrom().voidItem();
                })
                .invoke(bytes -> {
                    if (bytes != null) {
                        LOG.debugf("Redis HIT %s (%d bytes)", key, bytes.length);
                    } else {
                        LOG.debugf("Redis MISS %s", key);
                    }
                });
    }

    /**
     * Delete a document from the cache.
     *
     * @param nodeId the deterministic UUID
     * @return Uni completing when the delete is acknowledged
     */
    public Uni<Void> delete(String nodeId) {
        String key = KEY_PREFIX + nodeId;
        return redis.key().del(key)
                .invoke(count -> LOG.debugf("Redis DEL %s (removed=%d)", key, count))
                .replaceWithVoid();
    }
}
