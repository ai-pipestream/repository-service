package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import io.pipeline.repository.kafka.DriveEventEmitter;
import io.pipeline.repository.kafka.DocumentEventEmitter;
import io.pipeline.repository.kafka.SearchIndexEventEmitter;
import io.pipeline.repository.kafka.RequestCountEventEmitter;
import io.pipeline.repository.dto.DriveInfo;
import io.pipeline.repository.dto.DocumentInfo;
// import io.pipeline.repository.model.Drive;
// import io.pipeline.repository.model.Node;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Publisher for Kafka events using gRPC message types.
 * Handles all event publishing for OpenSearch indexing and analytics.
 */
@ApplicationScoped
public class EventPublisher {
    
    private static final Logger LOG = Logger.getLogger(EventPublisher.class);
    
    @Inject
    DriveEventEmitter driveEventEmitter;

    @Inject
    DocumentEventEmitter documentEventEmitter;

    @Inject
    SearchIndexEventEmitter searchIndexEventEmitter;

    @Inject
    RequestCountEventEmitter requestCountEventEmitter;

    // Drive events
    
    public Uni<Void> publishDriveCreated(DriveInfo drive) {
        DriveEvent event = DriveEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDriveName(drive.name())
            .setDriveId(drive.id())
            .setCustomerId(drive.customerId())
            .setCreated(DriveEvent.DriveCreated.newBuilder()
                .setBucketName(drive.bucketName())
                .setRegion(drive.region() != null ? drive.region() : "")
                .setDescription(drive.description() != null ? drive.description() : "")
                .build())
            .build();
        
        LOG.infof("Drive created event: %s (bucket: %s)", drive.name(), drive.bucketName());
        return driveEventEmitter.send(event)
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to publish drive created event: %s", drive.name()));
    }
    
    public Uni<Void> publishDriveUpdated(DriveInfo drive) {
        DriveEvent event = DriveEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDriveName(drive.name())
            .setDriveId(drive.id())
            .setCustomerId(drive.customerId())
            .setUpdated(DriveEvent.DriveUpdated.newBuilder()
                .setDescription(drive.description() != null ? drive.description() : "")
                .putAllMetadata(drive.metadata() != null ? drive.metadata() : java.util.Map.of())
                .build())
            .build();
        
        LOG.infof("Drive updated event: %s", drive.name());
        return driveEventEmitter.send(event)
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to publish drive updated event: %s", drive.name()))
            .replaceWithVoid();
    }
    
    public Uni<Void> publishDriveDeleted(DriveInfo drive) {
        DriveEvent event = DriveEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDriveName(drive.name())
            .setDriveId(drive.id())
            .setCustomerId(drive.customerId())
            .setDeleted(DriveEvent.DriveDeleted.newBuilder()
                .setReason("Drive deleted")
                .build())
            .build();
        
        LOG.infof("Drive deleted event: %s", drive.name());
        return driveEventEmitter.send(event)
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to publish drive deleted event: %s", drive.name()))
            .replaceWithVoid();
    }
    
    // Document events
    
    public Uni<Void> publishDocumentCreated(DocumentInfo document) {
        DocumentEvent event = DocumentEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(document.documentId())
            .setDriveName(document.driveName())
            .setDriveId(document.driveId())
            .setPath(document.path())
            .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                .setName(document.name())
                .setDocumentType(document.documentType())
                .setContentType(document.contentType() != null ? document.contentType() : "")
                .setSize(document.size() != null ? document.size() : 0)
                .setS3Key(document.s3Key() != null ? document.s3Key() : "")
                .setPayloadType(document.payloadType() != null ? document.payloadType() : "")
                .setPayload(document.payload() != null ? document.payload() : com.google.protobuf.Any.getDefaultInstance())
                .build())
            .build();
        
        // Also publish search index event
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(document.documentId())
            .setDriveName(document.driveName())
            .setIndexName("documents")
            .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                .setMetadata(document.payload() != null ? document.payload() : com.google.protobuf.Any.getDefaultInstance())
                .setDocumentType(document.documentType().toString())
                .setContentType(document.contentType() != null ? document.contentType() : "")
                .setSize(document.size() != null ? document.size() : 0)
                .setPath(document.path())
                .build())
            .build();

        LOG.infof("Document created event: %s (drive: %s, path: %s)", 
            document.documentId(), document.driveName(), document.path());
        LOG.infof("Search index event: %s", document.documentId());
        
        return Uni.combine().all().unis(
            documentEventEmitter.send(event),
            searchIndexEventEmitter.send(searchEvent)
        ).discardItems()
        .onFailure().invoke(e -> LOG.errorf(e, "Failed to publish document created event: %s", document.documentId()));
    }
    
    public Uni<Void> publishDocumentUpdated(String documentId, String driveName, Long driveId, String path,
                                           String name, String contentType, Long size, String payloadType, com.google.protobuf.Any payload) {
        DocumentEvent event = DocumentEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(documentId)
            .setDriveName(driveName)
            .setDriveId(driveId)
            .setPath(path)
            .setUpdated(DocumentEvent.DocumentUpdated.newBuilder()
                .setName(name)
                .setContentType(contentType != null ? contentType : "")
                .setSize(size != null ? size : 0)
                .setPayloadType(payloadType != null ? payloadType : "")
                .setPayload(payload != null ? payload : com.google.protobuf.Any.getDefaultInstance())
                .build())
            .build();
        
        // Also publish search index event
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(documentId)
            .setDriveName(driveName)
            .setIndexName("documents")
            .setIndexRequested(SearchIndexEvent.SearchIndexRequested.newBuilder()
                .setMetadata(payload != null ? payload : com.google.protobuf.Any.getDefaultInstance())
                .setDocumentType("FILE") // TODO: Get actual document type
                .setContentType(contentType != null ? contentType : "")
                .setSize(size != null ? size : 0)
                .setPath(path)
                .build())
            .build();
        
        LOG.infof("Document updated event: %s", documentId);
        LOG.infof("Search index event: %s", documentId);
        
        // TODO: Implement Kafka publishing
        return Uni.createFrom().voidItem();
    }
    
    /* public Uni<Void> publishDocumentDeleted(Node document) {
        DocumentEvent event = DocumentEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(document.documentId())
            .setDriveName(document.driveName())
            .setDriveId(document.getDrive().getId())
            .setPath(document.path())
            .setDeleted(DocumentEvent.DocumentDeleted.newBuilder()
                .setReason("Document deleted")
                .build())
            .build();
        
        // Also publish search index event
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDocumentId(document.documentId())
            .setDriveName(document.driveName())
            .setIndexName("documents")
            .setIndexDeleted(SearchIndexEvent.SearchIndexDeleted.newBuilder()
                .setReason("Document deleted")
                .build())
            .build();
        
        LOG.infof("Document deleted event: %s", document.documentId());
        LOG.infof("Search index deleted event: %s", document.documentId());
        
        // TODO: Implement Kafka publishing
        return Uni.createFrom().voidItem();
    }
    
    /* public Uni<Void> publishDocumentAccessed(Node document) {
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDriveName(document.driveName())
            .setDriveId(document.getDrive().getId())
            .setCustomerId(document.getDrive().getCustomerId())
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId(document.documentId())
                .setOperation("READ")
                .setBytesTransferred(document.size() != null ? document.size() : 0)
                .setProcessingTimeMs(0) // TODO: Track actual processing time
                .build())
            .build();

        LOG.infof("Document accessed event: %s (bytes: %d)", 
            document.documentId(), document.size() != null ? document.size() : 0);
        
        requestCountEventEmitter.sendAndForget(event);
        return Uni.createFrom().voidItem();
    }
    
    public Uni<Void> publishDocumentListAccessed(String driveName, int count) {
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestamp(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build())
            .setDriveName(driveName)
            .setListRequest(RequestCountEvent.ListRequestCount.newBuilder()
                .setPath("/")
                .setResultCount(count)
                .setProcessingTimeMs(0) // TODO: Track actual processing time
                .build())
            .build();
        
        LOG.infof("Document list accessed event: %s (count: %d)", driveName, count);
        
        requestCountEventEmitter.sendAndForget(event);
        return Uni.createFrom().voidItem();
    }
    
    // TODO: Implement remaining document methods with DocumentInfo records
    */
}