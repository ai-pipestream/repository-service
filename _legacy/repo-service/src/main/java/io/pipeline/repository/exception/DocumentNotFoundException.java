package io.pipeline.repository.exception;

/**
 * Thrown when a requested document/node cannot be found.
 */
public class DocumentNotFoundException extends RepoServiceException {
    
    public DocumentNotFoundException(String documentId) {
        super("DOCUMENT_NOT_FOUND", "findDocument", "Document not found: " + documentId);
    }
    
    public DocumentNotFoundException(String documentId, String location) {
        super("DOCUMENT_NOT_FOUND", "findDocument", 
            String.format("Document not found: %s (searched in: %s)", documentId, location));
    }
}
