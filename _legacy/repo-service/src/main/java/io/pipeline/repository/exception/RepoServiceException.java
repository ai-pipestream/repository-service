package io.pipeline.repository.exception;

/**
 * Base exception for all Repository Service operations.
 * Provides structured error handling for storage, database, and S3 operations.
 */
public class RepoServiceException extends RuntimeException {
    
    private final String errorCode;
    private final String operation;
    
    public RepoServiceException(String errorCode, String operation, String message) {
        super(String.format("[%s] %s: %s", errorCode, operation, message));
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    public RepoServiceException(String errorCode, String operation, String message, Throwable cause) {
        super(String.format("[%s] %s: %s", errorCode, operation, message), cause);
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getOperation() {
        return operation;
    }
    
    /**
     * Create a structured error message for gRPC responses.
     */
    public String getStructuredMessage() {
        return String.format("Repository Service Error - Code: %s, Operation: %s, Details: %s", 
            errorCode, operation, getMessage());
    }
}
