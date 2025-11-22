package io.pipeline.repository.exception;

/**
 * Thrown when database operations fail.
 */
public class DatabaseOperationException extends RepoServiceException {
    
    public DatabaseOperationException(String operation, String entityType, Throwable cause) {
        super("DATABASE_ERROR", operation, 
            String.format("Database operation failed on %s", entityType), cause);
    }
    
    public DatabaseOperationException(String operation, String entityType, String details) {
        super("DATABASE_ERROR", operation, 
            String.format("Database operation failed on %s: %s", entityType, details));
    }
    
    public static DatabaseOperationException persistFailed(String entityType, Throwable cause) {
        return new DatabaseOperationException("persist", entityType, cause);
    }
    
    public static DatabaseOperationException updateFailed(String entityType, Throwable cause) {
        return new DatabaseOperationException("update", entityType, cause);
    }
    
    public static DatabaseOperationException deleteFailed(String entityType, Throwable cause) {
        return new DatabaseOperationException("delete", entityType, cause);
    }
}
