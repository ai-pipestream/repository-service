package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.*;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;
import io.pipeline.repository.util.HashUtil;
import io.pipeline.repository.util.RequestContext;

/**
 * Reactive implementation of EventPublisher using the new RepositoryEvent structure.
 */
@ApplicationScoped
public class ReactiveEventPublisherImpl implements EventPublisher {

    private static final Logger LOG = Logger.getLogger(ReactiveEventPublisherImpl.class);
    
    @Inject
    @Channel("repository-events")
    MutinyEmitter<RepositoryEvent> repositoryEventEmitter;
    
    /**
     * Publish a document created event using the new lean RepositoryEvent structure.
     */
    @Override
    public Cancellable publishDocumentCreated(String documentId, String driveName, long driveId, 
                                             String path, String name, String contentType, 
                                             long size, String s3Key, String payloadType,
                                             String contentHash, String s3VersionId,
                                             String accountId, String connectorId) {
        return publishDocumentCreatedToTopic("repository-events", documentId, driveName, driveId,
            path, name, contentType, size, s3Key, payloadType, contentHash, s3VersionId, accountId, connectorId);
    }
    
    /**
     * Publish document created event to a specific topic (for testing).
     */
    public Cancellable publishDocumentCreatedToTopic(String topicName, String documentId, String driveName, long driveId, 
                                                    String path, String name, String contentType, 
                                                    long size, String s3Key, String payloadType,
                                                    String contentHash, String s3VersionId,
                                                    String accountId, String connectorId) {
        try {
            // Build SourceContext with proper values
            SourceContext source = SourceContext.newBuilder()
                .setComponent("repository-service")
                .setOperation("create")
                .setRequestId(RequestContext.getRequestId())
                .setConnectorId(connectorId != null ? connectorId : "")
                .build();
            
            // Build Created operation with minimal fields
            RepositoryEvent.Created created = RepositoryEvent.Created.newBuilder()
                .setS3Key(s3Key)
                .setSize(size)
                .setContentHash(contentHash != null ? contentHash : "")
                .setS3VersionId(s3VersionId != null ? s3VersionId : "")
                .build();
            
            // Generate deterministic event ID
            long timestampMillis = System.currentTimeMillis();
            String eventId = HashUtil.generateEventId(documentId, "create", timestampMillis);

            // Build main event with all required fields
            RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(timestampMillis / 1000)
                    .setNanos((int)((timestampMillis % 1000) * 1_000_000))
                    .build())
                .setDocumentId(documentId)
                .setAccountId(accountId != null ? accountId : "")
                .setSource(source)
                .setCreated(created)  // Use the oneof field
                .build();

            // Use deterministic partition key
            UUID messageKey = HashUtil.generatePartitionKey(documentId);
            
            Message<RepositoryEvent> message = Message.of(event)
                .addMetadata(OutgoingKafkaRecordMetadata.<UUID>builder()
                    .withKey(messageKey)
                    .withTopic(topicName)
                    .build());

            LOG.infof("Sending repository created event: documentId=%s, drive=%s, hash=%s, version=%s, topic=%s", 
                documentId, driveName, contentHash, s3VersionId, topicName);
            
            return repositoryEventEmitter.sendMessageAndForget(message);
                    
        } catch (Exception e) {
            LOG.errorf(e, "Error building repository created event: documentId=%s", documentId);
            throw new RuntimeException("Failed to build event", e);
        }
    }
    
    @Override
    public Cancellable publishDocumentUpdated(String documentId, String driveName, long driveId, 
                                             String path, String name, String contentType, 
                                             long size, String payloadType,
                                             String contentHash, String s3VersionId,
                                             String previousVersionId, String accountId) {
        try {
            // Look up Node to get actual S3 key
            io.pipeline.repository.entity.Node node = io.pipeline.repository.entity.Node.findByDocumentId(documentId);
            String s3Key = node != null && node.s3Key != null ? node.s3Key : documentId + ".pb";
            
            // Build SourceContext
            SourceContext source = SourceContext.newBuilder()
                .setComponent("repository-service")
                .setOperation("update")
                .setRequestId(RequestContext.getRequestId())
                .build();
            
            // Build Updated operation with minimal fields
            RepositoryEvent.Updated updated = RepositoryEvent.Updated.newBuilder()
                .setS3Key(s3Key)
                .setSize(size)
                .setContentHash(contentHash != null ? contentHash : "")
                .setS3VersionId(s3VersionId != null ? s3VersionId : "")
                .setPreviousVersionId(previousVersionId != null ? previousVersionId : "")
                .build();

            // Generate deterministic event ID
            long timestampMillis = System.currentTimeMillis();
            String eventId = HashUtil.generateEventId(documentId, "update", timestampMillis);

            // Build main event with all required fields
            RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(timestampMillis / 1000)
                    .setNanos((int)((timestampMillis % 1000) * 1_000_000))
                    .build())
                .setDocumentId(documentId)
                .setAccountId(accountId != null ? accountId : "")
                .setSource(source)
                .setUpdated(updated)  // Use the oneof field
                .build();

            // Use deterministic partition key
            UUID messageKey = HashUtil.generatePartitionKey(documentId);
            
            Message<RepositoryEvent> message = Message.of(event)
                .addMetadata(OutgoingKafkaRecordMetadata.<UUID>builder()
                    .withKey(messageKey)
                    .withTopic("repository-events")
                    .build());

            LOG.infof("Sending repository updated event: documentId=%s, hash=%s, version=%s->%s", 
                documentId, contentHash, previousVersionId, s3VersionId);
            return repositoryEventEmitter.sendMessageAndForget(message);
                    
        } catch (Exception e) {
            LOG.errorf(e, "Error building repository updated event: documentId=%s", documentId);
            throw new RuntimeException("Failed to build event", e);
        }
    }
    
    @Override
    public Cancellable publishDocumentDeleted(String documentId, String driveName, long driveId, 
                                             String path, String reason) {
        try {
            // Look up Drive to get accountId
            io.pipeline.repository.entity.Drive drive = io.pipeline.repository.entity.Drive.findById(driveId);
            String accountId = drive != null && drive.accountId != null ? drive.accountId : "";
            
            // Build SourceContext
            SourceContext source = SourceContext.newBuilder()
                .setComponent("repository-service")
                .setOperation("delete")
                .setRequestId(RequestContext.getRequestId())
                .build();
            
            // Build Deleted operation
            RepositoryEvent.Deleted deleted = RepositoryEvent.Deleted.newBuilder()
                .setReason(reason != null ? reason : "User requested deletion")
                .setPurged(false)  // TODO: Determine if S3 object was actually deleted
                .build();

            // Generate deterministic event ID
            long timestampMillis = System.currentTimeMillis();
            String eventId = HashUtil.generateEventId(documentId, "delete", timestampMillis);

            // Build main event
            RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(timestampMillis / 1000)
                    .setNanos((int)((timestampMillis % 1000) * 1_000_000))
                    .build())
                .setDocumentId(documentId)
                .setAccountId(accountId)
                .setSource(source)
                .setDeleted(deleted)  // Use the oneof field
                .build();

            UUID messageKey = HashUtil.generatePartitionKey(documentId);
            
            Message<RepositoryEvent> message = Message.of(event)
                .addMetadata(OutgoingKafkaRecordMetadata.<UUID>builder()
                    .withKey(messageKey)
                    .withTopic("repository-events")
                    .build());

            LOG.infof("Sending repository deleted event: documentId=%s, reason=%s", documentId, reason);
            return repositoryEventEmitter.sendMessageAndForget(message);
                    
        } catch (Exception e) {
            LOG.errorf(e, "Error building repository deleted event: documentId=%s", documentId);
            throw new RuntimeException("Failed to build event", e);
        }
    }
    
    @Override
    public Cancellable publishDocumentAccessed(String documentId, String driveName, long driveId, 
                                              String path, String accessType) {
        // Document access events are now moved to analytics/metrics, not Kafka
        LOG.debugf("Document accessed (not sent to Kafka): documentId=%s, type=%s", documentId, accessType);
        return () -> { /* No-op cancellable */ };
    }
    
    @Override
    public Cancellable publishSearchIndexRequested(String documentId, String indexName, String operation) {
        // Search index events are still needed for triggering indexing operations
        // These are lightweight metadata-only events that trigger the search indexer
        LOG.infof("Search index requested: documentId=%s, index=%s, op=%s", documentId, indexName, operation);
        
        // For now, return no-op until we define the SearchIndexEvent in the new proto schema
        // This will be a simple event with documentId, indexName, and operation
        return () -> { /* No-op cancellable - to be implemented with SearchIndexEvent proto */ };
    }
    
    @Override
    public Cancellable publishRequestCount(String documentId, String operation, long driveId) {
        // Request count events are now moved to metrics, not Kafka
        LOG.debugf("Request count (not sent to Kafka): documentId=%s, op=%s", documentId, operation);
        return () -> { /* No-op cancellable */ };
    }
    
    // Fire-and-forget convenience methods
    @Override
    public Cancellable publishDocumentCreatedAndForget(String documentId, String driveName, long driveId, 
                                                      String path, String name, String contentType, 
                                                      long size, String s3Key, String payloadType,
                                                      String contentHash, String s3VersionId,
                                                      String accountId, String connectorId) {
        // Just delegate to the main method - it's already fire-and-forget
        return publishDocumentCreated(documentId, driveName, driveId, path, name, contentType, 
                             size, s3Key, payloadType, contentHash, s3VersionId, accountId, connectorId);
    }
    
    @Override
    public Cancellable publishDocumentUpdatedAndForget(String documentId, String driveName, long driveId, 
                                                      String path, String name, String contentType, 
                                                      long size, String payloadType,
                                                      String contentHash, String s3VersionId,
                                                      String previousVersionId, String accountId) {
        // Just delegate to the main method - it's already fire-and-forget
        return publishDocumentUpdated(documentId, driveName, driveId, path, name, contentType, 
                             size, payloadType, contentHash, s3VersionId, previousVersionId, accountId);
    }
    
    @Override
    public Cancellable publishDocumentDeletedAndForget(String documentId, String driveName, long driveId, 
                                                      String path, String reason) {
        // Just delegate to the main method - it's already fire-and-forget
        return publishDocumentDeleted(documentId, driveName, driveId, path, reason);
    }
    
    
    // Helper method to construct S3 key in new format
    private String constructS3Key(String connectorId, String documentId) {
        return String.format("connectors/%s/%s.pb", 
            connectorId != null ? connectorId : "unknown", 
            documentId);
    }
}