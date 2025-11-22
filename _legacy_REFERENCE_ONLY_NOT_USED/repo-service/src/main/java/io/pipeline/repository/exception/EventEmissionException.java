package io.pipeline.repository.exception;

/**
 * Thrown when Kafka event emission fails.
 * This is typically a warning-level exception that doesn't fail the operation.
 */
public class EventEmissionException extends RepoServiceException {
    
    public EventEmissionException(String eventType, String topic, Throwable cause) {
        super("EVENT_EMISSION_ERROR", "emitEvent", 
            String.format("Failed to emit %s event to topic %s", eventType, topic), cause);
    }
    
    public EventEmissionException(String eventType, String topic, String details) {
        super("EVENT_EMISSION_ERROR", "emitEvent", 
            String.format("Failed to emit %s event to topic %s: %s", eventType, topic, details));
    }
    
    public static EventEmissionException kafkaUnavailable(String eventType, String topic) {
        return new EventEmissionException(eventType, topic, "Kafka cluster unavailable");
    }
    
    public static EventEmissionException serializationFailed(String eventType, Throwable cause) {
        return new EventEmissionException(eventType, "unknown", cause);
    }
}
