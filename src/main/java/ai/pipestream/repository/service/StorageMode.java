package ai.pipestream.repository.service;

/**
 * Document storage strategy for the repository service.
 * <p>
 * {@link #S3_ONLY} is the default — synchronous S3 write before ACK.
 * {@link #REDIS_BUFFERED} writes to Redis first (sub-ms), then flushes to S3 in the background.
 */
public enum StorageMode {
    S3_ONLY,
    REDIS_BUFFERED
}
