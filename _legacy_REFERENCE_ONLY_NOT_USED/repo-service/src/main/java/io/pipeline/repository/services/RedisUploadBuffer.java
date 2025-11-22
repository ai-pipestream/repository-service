package io.pipeline.repository.services;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis-based upload buffer for fast chunk storage.
 *
 * Stores incoming chunks temporarily in Redis with TTL,
 * then background worker assembles and uploads to S3.
 *
 * Key structure:
 *   upload:{nodeId}:metadata  → JSON metadata
 *   upload:{nodeId}:chunk:N   → Binary chunk data
 *   upload:{nodeId}:status    → RECEIVING|COMPLETE|UPLOADING|DONE|FAILED
 *   upload:{nodeId}:count     → Total chunk count
 */
@ApplicationScoped
public class RedisUploadBuffer {

    private static final Logger LOG = Logger.getLogger(RedisUploadBuffer.class);
    private static final Duration TTL = Duration.ofHours(1);
    private static final String UPLOAD_PREFIX = "upload:";

    @Inject
    ReactiveRedisDataSource redisDataSource;

    private ReactiveValueCommands<String, byte[]> binaryCommands;
    private ReactiveValueCommands<String, String> stringCommands;
    private ReactiveKeyCommands<String> keyCommands;

    @jakarta.annotation.PostConstruct
    void init() {
        this.binaryCommands = redisDataSource.value(byte[].class);
        this.stringCommands = redisDataSource.value(String.class);
        this.keyCommands = redisDataSource.key();
    }

    /**
     * Write a chunk to Redis with TTL.
     */
    public Uni<Void> writeChunk(String nodeId, int chunkNumber, byte[] data) {
        String chunkKey = buildChunkKey(nodeId, chunkNumber);
        long startTime = System.currentTimeMillis();

        LOG.infof("📝 Writing chunk to Redis: nodeId=%s, chunk=%d, size=%d bytes",
            nodeId, chunkNumber, data.length);

        return binaryCommands.setex(chunkKey, TTL.getSeconds(), data)
            .invoke(() -> {
                long duration = System.currentTimeMillis() - startTime;
                LOG.infof("✅ Chunk written to Redis: nodeId=%s, chunk=%d, duration=%dms",
                    nodeId, chunkNumber, duration);
            })
            .replaceWithVoid();
    }

    /**
     * Write upload metadata to Redis.
     */
    public Uni<Void> writeMetadata(String nodeId, String metadataJson) {
        String metadataKey = buildMetadataKey(nodeId);

        return stringCommands.setex(metadataKey, TTL.getSeconds(), metadataJson)
            .replaceWithVoid();
    }

    /**
     * Set chunk count for validation.
     */
    public Uni<Void> setChunkCount(String nodeId, int chunkCount) {
        String countKey = buildCountKey(nodeId);

        return stringCommands.setex(countKey, TTL.getSeconds(), String.valueOf(chunkCount))
            .replaceWithVoid();
    }

    /**
     * Mark upload as complete and ready for S3 upload.
     */
    public Uni<Void> markComplete(String nodeId) {
        String statusKey = buildStatusKey(nodeId);

        LOG.infof("✅ Marking upload complete in Redis: nodeId=%s", nodeId);

        return stringCommands.setex(statusKey, TTL.getSeconds(), "COMPLETE")
            .replaceWithVoid();
    }

    /**
     * Update status of upload (RECEIVING, COMPLETE, UPLOADING, DONE, FAILED).
     */
    public Uni<Void> updateStatus(String nodeId, String status) {
        String statusKey = buildStatusKey(nodeId);

        return stringCommands.setex(statusKey, TTL.getSeconds(), status)
            .replaceWithVoid();
    }

    /**
     * Read a single chunk from Redis.
     */
    public Uni<byte[]> readChunk(String nodeId, int chunkNumber) {
        String chunkKey = buildChunkKey(nodeId, chunkNumber);

        return binaryCommands.get(chunkKey)
            .onItem().ifNull().failWith(() ->
                new IllegalStateException("Chunk not found: " + chunkKey));
    }

    /**
     * Read all chunks from Redis in order.
     */
    public Uni<List<byte[]>> readAllChunks(String nodeId, int chunkCount) {
        LOG.infof("📥 Reading %d chunks from Redis: nodeId=%s", chunkCount, nodeId);
        long startTime = System.currentTimeMillis();

        List<Uni<byte[]>> chunkUnis = new ArrayList<>();
        for (int i = 1; i <= chunkCount; i++) {
            chunkUnis.add(readChunk(nodeId, i));
        }

        return Uni.join().all(chunkUnis).andFailFast()
            .invoke(chunks -> {
                long duration = System.currentTimeMillis() - startTime;
                long totalBytes = chunks.stream().mapToLong(chunk -> chunk.length).sum();
                LOG.infof("✅ All chunks read from Redis: nodeId=%s, %d chunks, %d bytes, duration=%dms",
                    nodeId, chunkCount, totalBytes, duration);
            });
    }

    /**
     * Read metadata from Redis.
     */
    public Uni<String> readMetadata(String nodeId) {
        String metadataKey = buildMetadataKey(nodeId);

        return stringCommands.get(metadataKey)
            .onItem().ifNull().failWith(() ->
                new IllegalStateException("Metadata not found: " + metadataKey));
    }

    /**
     * Read status from Redis.
     */
    public Uni<String> readStatus(String nodeId) {
        String statusKey = buildStatusKey(nodeId);

        return stringCommands.get(statusKey)
            .onItem().ifNull().continueWith("UNKNOWN");
    }

    /**
     * Get chunk count from Redis.
     */
    public Uni<Integer> getChunkCount(String nodeId) {
        String countKey = buildCountKey(nodeId);

        return stringCommands.get(countKey)
            .map(Integer::parseInt)
            .onItem().ifNull().failWith(() ->
                new IllegalStateException("Chunk count not found: " + countKey));
    }

    /**
     * Delete all keys for a completed upload.
     */
    public Uni<Void> cleanup(String nodeId) {
        String pattern = UPLOAD_PREFIX + nodeId + ":*";

        LOG.infof("🗑️  Cleaning up Redis keys: pattern=%s", pattern);

        // Delete all keys matching the pattern
        return keyCommands.keys(pattern)
            .chain(keys -> {
                if (keys.isEmpty()) {
                    LOG.warnf("No keys found to delete: pattern=%s", pattern);
                    return Uni.createFrom().voidItem();
                }

                LOG.infof("Deleting %d keys from Redis: nodeId=%s", keys.size(), nodeId);
                return keyCommands.del(keys.toArray(new String[0]))
                    .invoke(deletedCount ->
                        LOG.infof("✅ Deleted %d keys from Redis: nodeId=%s", deletedCount, nodeId))
                    .replaceWithVoid();
            });
    }

    // Key builders
    private String buildChunkKey(String nodeId, int chunkNumber) {
        return UPLOAD_PREFIX + nodeId + ":chunk:" + chunkNumber;
    }

    private String buildMetadataKey(String nodeId) {
        return UPLOAD_PREFIX + nodeId + ":metadata";
    }

    private String buildStatusKey(String nodeId) {
        return UPLOAD_PREFIX + nodeId + ":status";
    }

    private String buildCountKey(String nodeId) {
        return UPLOAD_PREFIX + nodeId + ":count";
    }
}
