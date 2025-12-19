package ai.pipestream.repository.service.state;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Manages upload state.
 * <p>
 * Phase-1 note: Phase 1 uses unary `SavePipeDoc` and an in-memory placeholder implementation.
 * Later phases may add durable upload session tracking (DB-backed) for multipart/streaming uploads.
 */
@ApplicationScoped
public class UploadStateManager {

    private static final Logger LOG = Logger.getLogger(UploadStateManager.class);

    public UploadStateManager() {
        LOG.info("UploadStateManager initialized");
    }

    // TODO: Implement upload session state management methods for multipart/streaming phases
}
