package io.pipeline.repository.exception;

/**
 * Thrown when upload operations fail.
 */
public class UploadException extends RepoServiceException {
    
    public UploadException(String operation, String nodeId, String message) {
        super("UPLOAD_ERROR", operation, 
            String.format("Upload failed for nodeId %s: %s", nodeId, message));
    }
    
    public UploadException(String operation, String nodeId, String message, Throwable cause) {
        super("UPLOAD_ERROR", operation, 
            String.format("Upload failed for nodeId %s: %s", nodeId, message), cause);
    }
    
    public static UploadException noChunks(String operation) {
        return new UploadException(operation, "unknown", "No chunks received");
    }
    
    public static UploadException progressNotFound(String operation, String nodeId) {
        return new UploadException(operation, nodeId, "Upload progress not found");
    }
    
    public static UploadException chunkProcessingFailed(String operation, String nodeId, Throwable cause) {
        return new UploadException(operation, nodeId, "Chunk processing failed", cause);
    }
    
    public static UploadException invalidChunk(String operation, String nodeId, String reason) {
        return new UploadException(operation, nodeId, "Invalid chunk: " + reason);
    }
}
