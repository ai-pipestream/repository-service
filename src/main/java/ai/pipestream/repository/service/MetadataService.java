package ai.pipestream.repository.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import jakarta.inject.Inject;

/**
 * Service for managing document metadata.
 * Handles metadata extraction, indexing, and retrieval.
 */
@ApplicationScoped
public class MetadataService {

    private static final Logger LOG = Logger.getLogger(MetadataService.class);

    public MetadataService() {
        LOG.info("MetadataService initialized");
    }

    /**
     * Persists document metadata from a RepositoryEvent.
     *
     * @param event The RepositoryEvent containing metadata.
     * @return A Uni containing the persisted Document.
     */
    @jakarta.transaction.Transactional
    public io.smallrye.mutiny.Uni<ai.pipestream.repository.entity.Document> persistMetadata(
            ai.pipestream.repository.filesystem.RepositoryEvent event) {
        if (!event.hasCreated()) {
            return io.smallrye.mutiny.Uni.createFrom().nullItem();
        }

        ai.pipestream.repository.filesystem.RepositoryEvent.Created created = event.getCreated();
        String docId = event.getDocumentId();

        ai.pipestream.repository.entity.Document doc = new ai.pipestream.repository.entity.Document();
        doc.documentId = docId;
        doc.connectorId = event.getSource().getConnectorId();
        if (doc.connectorId == null || doc.connectorId.isEmpty()) {
            // Fallback if connectorId is not in source, parse from key or use default
            // For now, we assume it's in the source or we extract from key?
            // Let's trust the source context for now, or default to "unknown"
            doc.connectorId = "unknown";
        }

        doc.bucketName = created.getBucketName();
        doc.storageLocation = created.getS3Key();
        doc.contentSize = created.getSize();
        doc.checksum = created.getContentHash();
        doc.version = created.getS3VersionId();
        doc.createdAt = java.time.Instant.ofEpochSecond(event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos());
        doc.updatedAt = java.time.Instant.now();
        doc.status = "ACTIVE";

        // These fields might need to be populated from elsewhere or defaults
        doc.title = docId; // Default title
        doc.contentType = "application/octet-stream"; // Default content type

        LOG.infof("Persisting metadata for doc: %s (Bucket: %s, Key: %s)", docId, doc.bucketName, doc.storageLocation);

        doc.persist();
        return io.smallrye.mutiny.Uni.createFrom().item(doc);
    }

    /**
     * Retrieves document metadata by connectorId and docId.
     *
     * @param connectorId The connector ID.
     * @param docId       The document ID.
     * @return A Uni containing the Document.
     */
    public io.smallrye.mutiny.Uni<ai.pipestream.repository.entity.Document> getMetadata(String connectorId,
            String docId) {
        return ai.pipestream.repository.entity.Document.find("connectorId = ?1 and documentId = ?2", connectorId, docId)
                .firstResult();
    }

    /**
     * Retrieves document metadata by docId only.
     *
     * @param docId The document ID.
     * @return A Uni containing the Document.
     */
    public io.smallrye.mutiny.Uni<ai.pipestream.repository.entity.Document> getMetadata(String docId) {
        return ai.pipestream.repository.entity.Document.find("documentId", docId).firstResult();
    }
}
