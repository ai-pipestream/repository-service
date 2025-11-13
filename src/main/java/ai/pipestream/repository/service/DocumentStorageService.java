package ai.pipestream.repository.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for managing document storage operations.
 * Handles CRUD operations for documents in the repository.
 */
@ApplicationScoped
public class DocumentStorageService {

    private static final Logger LOG = Logger.getLogger(DocumentStorageService.class);

    public DocumentStorageService() {
        LOG.info("DocumentStorageService initialized");
    }

    // TODO: Implement document storage operations
}
