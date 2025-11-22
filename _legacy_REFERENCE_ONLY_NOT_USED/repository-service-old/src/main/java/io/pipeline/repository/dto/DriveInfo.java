package io.pipeline.repository.dto;

import java.util.Map;

/**
 * Immutable data transfer object for drive information.
 * Used for event publishing and data transfer between layers.
 */
public record DriveInfo(
    String name,
    Long id,
    String customerId,
    String bucketName,
    String region,
    String description,
    Map<String, String> metadata
) {
    
    /**
     * Create DriveInfo with minimal required fields.
     */
    public static DriveInfo of(String name, Long id, String customerId, String bucketName) {
        return new DriveInfo(name, id, customerId, bucketName, null, null, null);
    }
    
    /**
     * Create DriveInfo with all fields.
     */
    public static DriveInfo of(String name, Long id, String customerId, String bucketName, 
                              String region, String description, Map<String, String> metadata) {
        return new DriveInfo(name, id, customerId, bucketName, region, description, metadata);
    }
}