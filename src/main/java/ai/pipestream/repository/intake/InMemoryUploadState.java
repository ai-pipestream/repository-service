package ai.pipestream.repository.intake;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ephemeral state for a single upload tracked in memory.
 * Used in Phase 1 for fast ACK pattern without persistence.
 */
public class InMemoryUploadState {

    private final String nodeId;
    private final String uploadId;
    private final Instant createdAt;
    private final ConcurrentHashMap.KeySetView<Integer, Boolean> receivedChunks;
    private final AtomicLong bytesReceivedTotal;
    private volatile Instant lastActivityTs;

    public InMemoryUploadState(String nodeId, String uploadId) {
        this.nodeId = nodeId;
        this.uploadId = uploadId;
        this.createdAt = Instant.now();
        this.receivedChunks = ConcurrentHashMap.newKeySet();
        this.bytesReceivedTotal = new AtomicLong(0);
        this.lastActivityTs = this.createdAt;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getUploadId() {
        return uploadId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public int getReceivedChunkCount() {
        return receivedChunks.size();
    }

    public long getBytesReceivedTotal() {
        return bytesReceivedTotal.get();
    }

    public Instant getLastActivityTs() {
        return lastActivityTs;
    }

    /**
     * Check if a chunk has already been received.
     *
     * @param chunkNumber the chunk number to check
     * @return true if chunk was already received
     */
    public boolean hasChunk(int chunkNumber) {
        return receivedChunks.contains(chunkNumber);
    }

    /**
     * Mark a chunk as received and update counters.
     *
     * @param chunkNumber the chunk number
     * @param byteCount   the size of the chunk in bytes
     * @return true if this was a new chunk, false if it was already received
     */
    public boolean receiveChunk(int chunkNumber, long byteCount) {
        boolean added = receivedChunks.add(chunkNumber);
        if (added) {
            bytesReceivedTotal.addAndGet(byteCount);
        }
        lastActivityTs = Instant.now();
        return added;
    }

    /**
     * Update the last activity timestamp without modifying chunk state.
     * Useful for tracking activity on status checks.
     */
    public void touch() {
        lastActivityTs = Instant.now();
    }
}
