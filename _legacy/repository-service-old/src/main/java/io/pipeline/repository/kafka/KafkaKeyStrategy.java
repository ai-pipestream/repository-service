package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Strategy for generating Kafka message keys based on entity types.
 * 
 * Key Strategy:
 * - Document-related events: Use documentId (PipeDoc ID) for consistent partitioning
 * - Drive events: Use driveId (best guess for non-document entities)
 * - Request count events: Use documentId when available, fallback to driveId
 * 
 * This ensures all events for the same PipeDoc are processed in order.
 */
@ApplicationScoped
public class KafkaKeyStrategy {
    
    private static final Logger LOG = Logger.getLogger(KafkaKeyStrategy.class);
    
    /**
     * Generate a UUID key for the given message based on its type.
     * 
     * @param message The protobuf message
     * @return UUID key for consistent partitioning
     * @throws IllegalArgumentException if message type is unknown
     */
    public UUID getKey(Object message) {
        if (message instanceof DriveEvent) {
            return forDriveEvent((DriveEvent) message);
        }
        if (message instanceof DocumentEvent) {
            return forDocumentEvent((DocumentEvent) message);
        }
        if (message instanceof SearchIndexEvent) {
            return forSearchIndexEvent((SearchIndexEvent) message);
        }
        if (message instanceof RequestCountEvent) {
            return forRequestCountEvent((RequestCountEvent) message);
        }
        
        LOG.errorf("Unknown message type for key generation: %s", message.getClass().getName());
        throw new IllegalArgumentException("Unknown message type: " + message.getClass());
    }
    
    /**
     * Generate key for DriveEvent using driveId.
     * Best guess for non-document entities.
     */
    private UUID forDriveEvent(DriveEvent event) {
        String keySource = String.valueOf(event.getDriveId());
        UUID key = UUID.nameUUIDFromBytes(keySource.getBytes());
        LOG.debugf("Generated key for DriveEvent: driveId=%s, key=%s", event.getDriveId(), key);
        return key;
    }
    
    /**
     * Generate key for DocumentEvent using documentId (PipeDoc ID).
     * Primary document entity - all related events should use this key.
     */
    private UUID forDocumentEvent(DocumentEvent event) {
        String keySource = event.getDocumentId();
        UUID key = UUID.nameUUIDFromBytes(keySource.getBytes());
        LOG.debugf("Generated key for DocumentEvent: documentId=%s, key=%s", event.getDocumentId(), key);
        return key;
    }
    
    /**
     * Generate key for SearchIndexEvent using documentId (PipeDoc ID).
     * Document-related event - should partition with DocumentEvent.
     */
    private UUID forSearchIndexEvent(SearchIndexEvent event) {
        String keySource = event.getDocumentId();
        UUID key = UUID.nameUUIDFromBytes(keySource.getBytes());
        LOG.debugf("Generated key for SearchIndexEvent: documentId=%s, key=%s", event.getDocumentId(), key);
        return key;
    }
    
    /**
     * Generate key for RequestCountEvent.
     * Use documentId when available (document-related), fallback to driveId.
     */
    private UUID forRequestCountEvent(RequestCountEvent event) {
        UUID key;
        String keySource;
        
        if (event.hasDocumentRequest()) {
            // Document-related request count → same partition as document events
            keySource = event.getDocumentRequest().getDocumentId();
            key = UUID.nameUUIDFromBytes(keySource.getBytes());
            LOG.debugf("Generated key for RequestCountEvent (document): documentId=%s, key=%s", keySource, key);
        } else {
            // Drive-related request count → use driveId
            keySource = String.valueOf(event.getDriveId());
            key = UUID.nameUUIDFromBytes(keySource.getBytes());
            LOG.debugf("Generated key for RequestCountEvent (drive): driveId=%s, key=%s", keySource, key);
        }
        
        return key;
    }
}