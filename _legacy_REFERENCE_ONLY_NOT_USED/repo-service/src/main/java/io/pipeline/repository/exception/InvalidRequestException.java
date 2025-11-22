package io.pipeline.repository.exception;

/**
 * Thrown when request validation fails.
 */
public class InvalidRequestException extends RepoServiceException {
    
    public InvalidRequestException(String operation, String field, String reason) {
        super("INVALID_REQUEST", operation, 
            String.format("Invalid %s: %s", field, reason));
    }
    
    public InvalidRequestException(String operation, String message) {
        super("INVALID_REQUEST", operation, message);
    }
    
    public static InvalidRequestException missingField(String operation, String fieldName) {
        return new InvalidRequestException(operation, fieldName, "field is required but missing");
    }
    
    public static InvalidRequestException invalidField(String operation, String fieldName, String value, String reason) {
        return new InvalidRequestException(operation, fieldName, 
            String.format("value '%s' is invalid: %s", value, reason));
    }
    
    public static InvalidRequestException emptyPayload(String operation) {
        return new InvalidRequestException(operation, "payload", "payload cannot be empty");
    }
}
