package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Kafka consumer for integration tests.
 * 
 * This consumer runs in the integration test environment and collects
 * messages from Kafka topics to verify that events are being published correctly.
 */
public class IntegrationTestKafkaConsumer {

    private static final Logger LOG = Logger.getLogger(IntegrationTestKafkaConsumer.class);

    // Static collections to store consumed messages
    private static final List<KafkaRecord<UUID, DriveEvent>> driveEvents = new CopyOnWriteArrayList<>();
    private static final List<KafkaRecord<UUID, DocumentEvent>> documentEvents = new CopyOnWriteArrayList<>();
    private static final List<KafkaRecord<UUID, SearchIndexEvent>> searchIndexEvents = new CopyOnWriteArrayList<>();
    private static final List<KafkaRecord<UUID, RequestCountEvent>> requestCountEvents = new CopyOnWriteArrayList<>();

    /**
     * Consume DriveEvent messages from the drive-events topic.
     */
    @Incoming("drive-events")
    public io.smallrye.mutiny.Uni<Void> consumeDriveEvent(KafkaRecord<UUID, DriveEvent> record) {
        LOG.infof("Integration test consumed DriveEvent: key=%s, driveId=%d", 
                record.getKey(), record.getPayload().getDriveId());
        driveEvents.add(record);
        return io.smallrye.mutiny.Uni.createFrom().voidItem();
    }

    /**
     * Consume DocumentEvent messages from the document-events topic.
     */
    @Incoming("document-events")
    public io.smallrye.mutiny.Uni<Void> consumeDocumentEvent(KafkaRecord<UUID, DocumentEvent> record) {
        LOG.infof("Integration test consumed DocumentEvent: key=%s, documentId=%s", 
                record.getKey(), record.getPayload().getDocumentId());
        documentEvents.add(record);
        return io.smallrye.mutiny.Uni.createFrom().voidItem();
    }

    /**
     * Consume SearchIndexEvent messages from the search-index-events topic.
     */
    @Incoming("search-index-events")
    public io.smallrye.mutiny.Uni<Void> consumeSearchIndexEvent(KafkaRecord<UUID, SearchIndexEvent> record) {
        LOG.infof("Integration test consumed SearchIndexEvent: key=%s, documentId=%s", 
                record.getKey(), record.getPayload().getDocumentId());
        searchIndexEvents.add(record);
        return io.smallrye.mutiny.Uni.createFrom().voidItem();
    }

    /**
     * Consume RequestCountEvent messages from the request-count-events topic.
     */
    @Incoming("request-count-events")
    public io.smallrye.mutiny.Uni<Void> consumeRequestCountEvent(KafkaRecord<UUID, RequestCountEvent> record) {
        LOG.infof("Integration test consumed RequestCountEvent: key=%s, driveId=%d", 
                record.getKey(), record.getPayload().getDriveId());
        requestCountEvents.add(record);
        return io.smallrye.mutiny.Uni.createFrom().voidItem();
    }

    // Getters for test access
    public static List<KafkaRecord<UUID, DriveEvent>> getDriveEvents() {
        return driveEvents;
    }

    public static List<KafkaRecord<UUID, DocumentEvent>> getDocumentEvents() {
        return documentEvents;
    }

    public static List<KafkaRecord<UUID, SearchIndexEvent>> getSearchIndexEvents() {
        return searchIndexEvents;
    }

    public static List<KafkaRecord<UUID, RequestCountEvent>> getRequestCountEvents() {
        return requestCountEvents;
    }

    /**
     * Clear all consumed messages. Call this before each test.
     */
    public static void clearAllMessages() {
        LOG.info("Clearing all consumed messages for integration test");
        driveEvents.clear();
        documentEvents.clear();
        searchIndexEvents.clear();
        requestCountEvents.clear();
    }

    /**
     * Get total count of all consumed messages.
     */
    public static int getTotalMessageCount() {
        return driveEvents.size() + documentEvents.size() + searchIndexEvents.size() + requestCountEvents.size();
    }

    /**
     * Wait for a specific number of messages to be consumed.
     */
    public static void waitForMessages(int expectedCount, long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (getTotalMessageCount() >= expectedCount) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}