package io.pipeline.repository.dto;

import com.google.protobuf.Any;

/**
 * Immutable data transfer object for document information.
 * Used for event publishing and data transfer between layers.
 */
public record DocumentInfo(
    String documentId,
    String driveName,
    Long driveId,
    String path,
    String name,
    String documentType,
    String contentType,
    Long size,
    String s3Key,
    String payloadType,
    Any payload
) {
    
    /**
     * Create DocumentInfo with minimal required fields.
     */
    public static DocumentInfo of(String documentId, String driveName, Long driveId, String name) {
        return new DocumentInfo(documentId, driveName, driveId, "/", name, "FILE", null, null, null, null, null);
    }
    
    /**
     * Create DocumentInfo with all fields.
     */
    public static DocumentInfo of(String documentId, String driveName, Long driveId, String path,
                                 String name, String documentType, String contentType, Long size,
                                 String s3Key, String payloadType, Any payload) {
        return new DocumentInfo(documentId, driveName, driveId, path, name, documentType, 
                               contentType, size, s3Key, payloadType, payload);
    }
    
    /**
     * Create DocumentInfo without payload (for metadata-only operations).
     */
    public static DocumentInfo withoutPayload(String documentId, String driveName, Long driveId, String path,
                                            String name, String documentType, String contentType, Long size,
                                            String s3Key, String payloadType) {
        return new DocumentInfo(documentId, driveName, driveId, path, name, documentType,
                               contentType, size, s3Key, payloadType, null);
    }
}