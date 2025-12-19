package ai.pipestream.repository.worker;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Background worker that processes uploaded chunks and uploads to S3 (multipart/streaming phases).
 * 
 * Flow:
 * 1. Consume chunk notifications / session state
 * 2. Claim chunks from the coordinator store (DB-backed)
 * 3. Upload to S3 as multipart
 * 4. Store ETags in the coordinator store (DB-backed)
 * 5. Handle encryption for SSE-C
 * <p>
 * Design reference: docs/new-design/03-s3-multipart.md
 */
@ApplicationScoped
public class ChunkProcessorWorker {

    private static final Logger LOG = Logger.getLogger(ChunkProcessorWorker.class);

    public ChunkProcessorWorker() {
        LOG.info("ChunkProcessorWorker initialized");
    }

    // TODO: Implement chunk processing
    // - Subscribe to chunk notifications/session state
    // - Claim chunk (atomic claim in coordinator store)
    // - Upload part to S3
    // - Store ETag in coordinator store
    // - Update progress
    // - Retry logic with exponential backoff
}
