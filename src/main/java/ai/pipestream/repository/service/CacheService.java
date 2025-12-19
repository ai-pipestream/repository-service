package ai.pipestream.repository.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for managing cache operations.
 * Handles caching of frequently accessed documents and metadata (implementation TBD).
 */
@ApplicationScoped
public class CacheService {

    private static final Logger LOG = Logger.getLogger(CacheService.class);

    public CacheService() {
        LOG.info("CacheService initialized");
    }

    // TODO: Implement cache operations
}
