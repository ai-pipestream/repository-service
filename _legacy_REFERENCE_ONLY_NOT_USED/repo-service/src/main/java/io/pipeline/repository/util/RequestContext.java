package io.pipeline.repository.util;

import org.slf4j.MDC;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

/**
 * Utility for managing request context and generating deterministic request IDs.
 * Uses MDC (Mapped Diagnostic Context) for request tracing across the entire request lifecycle.
 */
public class RequestContext {
    
    private static final String REQUEST_ID_KEY = "requestId";
    private static final String CONNECTOR_ID_KEY = "connectorId";
    private static final String CUSTOMER_ID_KEY = "customerId";
    private static final String OPERATION_KEY = "operation";
    
    /**
     * Generate a deterministic request ID based on context.
     * Format: deterministic UUID based on: timestamp + operation + connectorId + randomSuffix
     */
    public static String generateRequestId(String operation, String connectorId) {
        long timestamp = System.currentTimeMillis();
        String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
        
        // Create composite string for deterministic UUID
        String composite = String.format("%d|%s|%s|%s", 
            timestamp, 
            operation != null ? operation : "unknown",
            connectorId != null ? connectorId : "system",
            randomSuffix
        );
        
        // Generate deterministic UUID from composite
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8)).toString();
    }
    
    /**
     * Set request context in MDC.
     */
    public static void setContext(String requestId, String operation, String connectorId, String customerId) {
        if (requestId != null) {
            MDC.put(REQUEST_ID_KEY, requestId);
        }
        if (operation != null) {
            MDC.put(OPERATION_KEY, operation);
        }
        if (connectorId != null) {
            MDC.put(CONNECTOR_ID_KEY, connectorId);
        }
        if (customerId != null) {
            MDC.put(CUSTOMER_ID_KEY, customerId);
        }
    }
    
    /**
     * Get current request ID from MDC, or generate a new one if not present.
     */
    public static String getRequestId() {
        String requestId = MDC.get(REQUEST_ID_KEY);
        if (requestId == null) {
            // Generate a new request ID if not in context
            String operation = MDC.get(OPERATION_KEY);
            String connectorId = MDC.get(CONNECTOR_ID_KEY);
            requestId = generateRequestId(operation, connectorId);
            MDC.put(REQUEST_ID_KEY, requestId);
        }
        return requestId;
    }
    
    /**
     * Get current connector ID from MDC.
     */
    public static String getConnectorId() {
        return MDC.get(CONNECTOR_ID_KEY);
    }
    
    /**
     * Get current customer ID from MDC.
     */
    public static String getCustomerId() {
        return MDC.get(CUSTOMER_ID_KEY);
    }
    
    /**
     * Get current operation from MDC.
     */
    public static String getOperation() {
        return MDC.get(OPERATION_KEY);
    }
    
    /**
     * Clear the MDC context (should be called at the end of request processing).
     */
    public static void clear() {
        MDC.remove(REQUEST_ID_KEY);
        MDC.remove(CONNECTOR_ID_KEY);
        MDC.remove(CUSTOMER_ID_KEY);
        MDC.remove(OPERATION_KEY);
    }
}