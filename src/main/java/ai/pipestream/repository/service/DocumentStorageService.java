package ai.pipestream.repository.service;

import ai.pipestream.data.v1.PipeDoc;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing document storage operations.
 * Handles CRUD operations for documents in the repository.
 */
@ApplicationScoped
public class DocumentStorageService {

    private static final Logger LOG = Logger.getLogger(DocumentStorageService.class);

    private final ConcurrentHashMap<String, PipeDoc> inMemoryDocs = new ConcurrentHashMap<>();

    public DocumentStorageService() {
        LOG.info("DocumentStorageService initialized");
    }

    /**
     * Store a {@link PipeDoc}. This is a Phase-1 placeholder implementation (in-memory only).
     *
     * @param document the document to store
     * @return storage result containing the resolved document id and a synthetic S3 key
     */
    public StoredDocument store(PipeDoc document) {
        if (document == null) {
            throw new IllegalArgumentException("document must not be null");
        }

        String documentId = document.getDocId();
        if (documentId == null || documentId.isBlank()) {
            documentId = UUID.randomUUID().toString();
            document = document.toBuilder().setDocId(documentId).build();
        }

        inMemoryDocs.put(documentId, document);
        String s3Key = "documents/" + documentId + ".pb";
        return new StoredDocument(documentId, s3Key);
    }

    public Optional<PipeDoc> get(String documentId) {
        if (documentId == null || documentId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(inMemoryDocs.get(documentId));
    }

    public void clear() {
        inMemoryDocs.clear();
    }

    public record StoredDocument(String documentId, String s3Key) {}
}
