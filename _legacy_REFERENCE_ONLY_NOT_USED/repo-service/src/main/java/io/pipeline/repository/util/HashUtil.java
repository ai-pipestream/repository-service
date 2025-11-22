package io.pipeline.repository.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

/**
 * Utility class for hash operations and deterministic ID generation.
 */
public class HashUtil {
    
    private static final String SHA_256 = "SHA-256";
    
    /**
     * Calculate SHA-256 hash of byte array content.
     * 
     * @param content The content to hash
     * @return Base64 encoded SHA-256 hash
     */
    public static String calculateSHA256(byte[] content) {
        if (content == null || content.length == 0) {
            return "EMPTY";
        }
        
        try {
            MessageDigest digest = MessageDigest.getInstance(SHA_256);
            byte[] hash = digest.digest(content);
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            // Should never happen as SHA-256 is guaranteed to be available
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
    
    /**
     * Generate a deterministic event ID based on document ID, operation, and timestamp.
     * 
     * @param documentId The document ID
     * @param operation The operation (create, update, delete)
     * @param timestampMillis The timestamp in milliseconds
     * @return Deterministic UUID as string
     */
    public static String generateEventId(String documentId, String operation, long timestampMillis) {
        String composite = documentId + "|" + operation + "|" + timestampMillis;
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8)).toString();
    }
    
    /**
     * Generate a deterministic document ID based on content and context.
     * Note: In the new architecture, this is only used as a fallback if client doesn't provide an ID.
     * 
     * @param customerId The customer ID
     * @param connectorId The connector ID
     * @param contentHash The content hash
     * @return Deterministic UUID as string
     */
    public static String generateDocumentId(String customerId, String connectorId, String contentHash) {
        String composite = customerId + "|" + connectorId + "|" + contentHash;
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8)).toString();
    }
    
    /**
     * Generate a deterministic partition key for Kafka.
     * 
     * @param documentId The document ID
     * @return UUID suitable for use as Kafka partition key
     */
    public static UUID generatePartitionKey(String documentId) {
        try {
            // If documentId is already a UUID, use it directly
            return UUID.fromString(documentId);
        } catch (IllegalArgumentException e) {
            // Otherwise, generate a deterministic UUID from the documentId
            return UUID.nameUUIDFromBytes(documentId.getBytes(StandardCharsets.UTF_8));
        }
    }
}