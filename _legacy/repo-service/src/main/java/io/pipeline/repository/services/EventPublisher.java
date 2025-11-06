package io.pipeline.repository.services;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.pipeline.repository.filesystem.*;

/**
 * Interface for publishing repository events.
 * Implementations can be reactive or blocking as needed.
 */
public interface EventPublisher {
    
    /**
     * Publish a document created event with new required fields.
     */
    Cancellable publishDocumentCreated(String documentId, String driveName, long driveId, 
                                      String path, String name, String contentType, 
                                      long size, String s3Key, String payloadType,
                                      String contentHash, String s3VersionId,
                                      String accountId, String connectorId);
    
    /**
     * Publish a document updated event with new required fields.
     */
    Cancellable publishDocumentUpdated(String documentId, String driveName, long driveId, 
                                      String path, String name, String contentType, 
                                      long size, String payloadType,
                                      String contentHash, String s3VersionId, 
                                      String previousVersionId, String accountId);
    
    /**
     * Publish a document deleted event.
     */
    Cancellable publishDocumentDeleted(String documentId, String driveName, long driveId, 
                                      String path, String reason);
    
    /**
     * Publish a document accessed event.
     */
    Cancellable publishDocumentAccessed(String documentId, String driveName, long driveId, 
                                       String path, String accessType);
    
    /**
     * Publish a search index requested event.
     */
    Cancellable publishSearchIndexRequested(String documentId, String indexName, String operation);
    
    /**
     * Publish a request count event for analytics.
     */
    Cancellable publishRequestCount(String documentId, String operation, long driveId);
    
    // Fire-and-forget convenience methods that return Cancellable for control
    Cancellable publishDocumentCreatedAndForget(String documentId, String driveName, long driveId, 
                                               String path, String name, String contentType, 
                                               long size, String s3Key, String payloadType,
                                               String contentHash, String s3VersionId,
                                               String accountId, String connectorId);
    
    // Custom topic methods for testing
    Cancellable publishDocumentCreatedToTopic(String topicName, String documentId, String driveName, long driveId, 
                                             String path, String name, String contentType, 
                                             long size, String s3Key, String payloadType,
                                             String contentHash, String s3VersionId,
                                             String accountId, String connectorId);
                                        
    Cancellable publishDocumentUpdatedAndForget(String documentId, String driveName, long driveId, 
                                               String path, String name, String contentType, 
                                               long size, String payloadType,
                                               String contentHash, String s3VersionId,
                                               String previousVersionId, String accountId);
                                        
    Cancellable publishDocumentDeletedAndForget(String documentId, String driveName, long driveId, 
                                               String path, String reason);
}