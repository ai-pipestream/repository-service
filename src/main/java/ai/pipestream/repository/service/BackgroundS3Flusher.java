package ai.pipestream.repository.service;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.v1.CacheFlushEvent;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import io.vertx.core.Context;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka-driven storage flusher for Redis-buffered documents.
 * <p>
 * When the repository stores a document in Redis (redis-buffered mode),
 * it emits a {@link CacheFlushEvent} to the {@code cache-flush} Kafka topic.
 * This consumer picks up the event and:
 * <ol>
 *   <li>Reads the protobuf bytes from Redis</li>
 *   <li>Writes to durable storage</li>
 *   <li>Updates DB status from PENDING_STORAGE to AVAILABLE</li>
 * </ol>
 * <p>
 * If Redis returns a cache miss (TTL expired), the record is marked LOST.
 * The source connector can re-crawl to recover lost documents.
 * <p>
 * Fully reactive — Redis GET, storage write, and DB update are all async.
 */
@ApplicationScoped
public class BackgroundS3Flusher {

    private static final Logger LOG = Logger.getLogger(BackgroundS3Flusher.class);

    public static final String STATUS_PENDING_STORAGE = "PENDING_STORAGE";
    public static final String STATUS_AVAILABLE = "AVAILABLE";
    public static final String STATUS_LOST = "LOST";

    @Inject S3AsyncClient s3AsyncClient;
    @Inject RedisDocumentCache redisCache;
    @Inject DriveService driveService;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    private final AtomicLong totalFlushed = new AtomicLong(0);
    private final AtomicLong totalLost = new AtomicLong(0);

    /**
     * Consumes flush events from the {@code cache-flush} Kafka topic.
     * Channel name {@code cache-flush-in} auto-derives topic {@code cache-flush}.
     * The Apicurio plugin auto-configures protobuf serde and failure-strategy=ignore.
     */
    @Incoming("cache-flush-in")
    public Uni<Void> consumeFlushEvent(Record<UUID, CacheFlushEvent> record) {
        CacheFlushEvent event = record.value();
        if (event == null) {
            LOG.warn("Received null flush event, skipping");
            return Uni.createFrom().voidItem();
        }

        String nodeIdStr = event.getNodeId();
        UUID nodeId;
        try {
            nodeId = UUID.fromString(nodeIdStr);
        } catch (IllegalArgumentException e) {
            LOG.warnf("Invalid node_id in flush event: %s", nodeIdStr);
            return Uni.createFrom().voidItem();
        }

        LOG.debugf("Flush event received: node_id=%s, object_key=%s", nodeIdStr, event.getObjectKey());

        return redisCache.get(nodeIdStr)
                .flatMap(bytes -> {
                    if (bytes == null) {
                        LOG.warnf("Flusher: Redis miss for node_id=%s (TTL expired) — marking LOST", nodeIdStr);
                        totalLost.incrementAndGet();
                        return markStatus(nodeId, STATUS_LOST);
                    }

                    return flushToStorage(nodeId, event, bytes);
                })
                .onFailure().invoke(e ->
                        LOG.errorf(e, "Flusher: failed to process flush event for node_id=%s", nodeIdStr))
                .onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    private Uni<Void> flushToStorage(UUID nodeId, CacheFlushEvent event, byte[] bytes) {
        // Capture current Vertx context — S3 async callback lands on AWS Netty thread,
        // so we must emitOn back to a Vertx context before touching Panache.
        Context callerContext = io.vertx.core.Vertx.currentContext();

        return driveService.resolveDrive(event.getDriveName(), event.getAccountId())
                .flatMap(resolvedDrive ->
                        Uni.createFrom().completionStage(
                                s3AsyncClient.putObject(
                                        PutObjectRequest.builder()
                                                .bucket(resolvedDrive.bucket())
                                                .key(event.getObjectKey())
                                                .contentType("application/x-protobuf")
                                                .contentLength((long) bytes.length)
                                                .build(),
                                        AsyncRequestBody.fromBytes(bytes))
                        )
                )
                .emitOn(runnable -> {
                    if (callerContext != null) callerContext.runOnContext(v -> runnable.run());
                    else vertx.getDelegate().getOrCreateContext().runOnContext(v -> runnable.run());
                })
                .flatMap(putResp -> {
                    totalFlushed.incrementAndGet();
                    LOG.debugf("Flusher: flushed node_id=%s to storage (%d bytes, etag=%s)",
                            nodeId, bytes.length, putResp.eTag());
                    return markStatus(nodeId, STATUS_AVAILABLE);
                });
    }

    private Uni<Void> markStatus(UUID nodeId, String status) {
        return Panache.withTransaction(() ->
                PipeDocRecord.<PipeDocRecord>findById(nodeId)
                        .onItem().ifNotNull().invoke(r -> r.status = status)
        ).replaceWithVoid();
    }

    public long getTotalFlushed() {
        return totalFlushed.get();
    }

    public long getTotalLost() {
        return totalLost.get();
    }
}
