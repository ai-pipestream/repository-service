package ai.pipestream.repository.intake;

import ai.pipestream.repository.config.IntakeConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory store for upload states with bounded size and TTL-based eviction.
 * Used in Phase 1 for fast ACK pattern without persistence.
 */
@ApplicationScoped
public class InMemoryUploadStateStore {

    private static final Logger LOG = Logger.getLogger(InMemoryUploadStateStore.class);

    @Inject
    IntakeConfiguration config;

    @Inject
    MeterRegistry meterRegistry;

    private Cache<String, InMemoryUploadState> cache;
    private final ConcurrentHashMap<String, String> nodeIdToUploadId = new ConcurrentHashMap<>();
    private final AtomicLong evictedTotal = new AtomicLong(0);

    @PostConstruct
    void init() {
        RemovalListener<String, InMemoryUploadState> removalListener = notification -> {
            if (notification.wasEvicted()) {
                evictedTotal.incrementAndGet();
                // Clean up nodeId index
                if (notification.getValue() != null) {
                    nodeIdToUploadId.remove(notification.getValue().getNodeId());
                }
                LOG.debugf("Upload state evicted: uploadId=%s, reason=%s",
                    notification.getKey(), notification.getCause());
            }
        };

        cache = CacheBuilder.newBuilder()
                .maximumSize(config.memory().maxUploads())
                .expireAfterAccess(config.memory().idleTtl().toMillis(), TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .recordStats()
                .build();

        // Register cache metrics
        GuavaCacheMetrics.monitor(meterRegistry, cache, "intake_upload_state");

        // Register custom gauges
        meterRegistry.gauge("in_memory_uploads_active", cache, Cache::size);
        meterRegistry.gauge("in_memory_uploads_evicted_total", evictedTotal, AtomicLong::get);

        LOG.infof("InMemoryUploadStateStore initialized: maxUploads=%d, idleTtl=%s",
                config.memory().maxUploads(), config.memory().idleTtl());
    }

    /**
     * Create a new upload state entry.
     *
     * @param nodeId   the node ID
     * @param uploadId the upload ID
     * @return the created state
     */
    public InMemoryUploadState create(String nodeId, String uploadId) {
        InMemoryUploadState state = new InMemoryUploadState(nodeId, uploadId);
        cache.put(uploadId, state);
        nodeIdToUploadId.put(nodeId, uploadId);
        LOG.debugf("Created upload state: nodeId=%s, uploadId=%s", nodeId, uploadId);
        return state;
    }

    /**
     * Get an upload state by upload ID.
     *
     * @param uploadId the upload ID
     * @return the state if present
     */
    public Optional<InMemoryUploadState> get(String uploadId) {
        InMemoryUploadState state = cache.getIfPresent(uploadId);
        return Optional.ofNullable(state);
    }

    /**
     * Get an upload state by node ID.
     *
     * @param nodeId the node ID
     * @return the state if present
     */
    public Optional<InMemoryUploadState> getByNodeId(String nodeId) {
        String uploadId = nodeIdToUploadId.get(nodeId);
        if (uploadId == null) {
            return Optional.empty();
        }
        return get(uploadId);
    }

    /**
     * Remove an upload state.
     *
     * @param uploadId the upload ID
     */
    public void remove(String uploadId) {
        InMemoryUploadState state = cache.getIfPresent(uploadId);
        if (state != null) {
            nodeIdToUploadId.remove(state.getNodeId());
        }
        cache.invalidate(uploadId);
        LOG.debugf("Removed upload state: uploadId=%s", uploadId);
    }

    /**
     * Get the current number of active uploads.
     *
     * @return the count
     */
    public long getActiveCount() {
        return cache.size();
    }

    /**
     * Get the total number of evicted uploads since startup.
     *
     * @return the count
     */
    public long getEvictedTotal() {
        return evictedTotal.get();
    }

    /**
     * Clean up expired entries. Called periodically or on demand.
     */
    public void cleanup() {
        cache.cleanUp();
    }
}
