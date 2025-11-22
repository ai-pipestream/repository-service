package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import io.pipeline.repository.kafka.DriveEventEmitter;
import io.pipeline.repository.kafka.DocumentEventEmitter;
import io.pipeline.repository.kafka.SearchIndexEventEmitter;
import io.pipeline.repository.kafka.RequestCountEventEmitter;
import io.pipeline.repository.kafka.KafkaKeyStrategy;
import io.pipeline.repository.services.EventPublisher;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for Apicurio Kafka integration.
 * 
 * This test validates the basic functionality of the Kafka key strategy
 * without requiring Quarkus or any infrastructure.
 */
public class ApicurioKafkaIntegrationTest {

    @Test
    void shouldGenerateConsistentKeysForDocumentEvents() {
        // Given
        KafkaKeyStrategy keyStrategy = new KafkaKeyStrategy();
        String documentId = "doc-12345";
        DocumentEvent event = DocumentEvent.newBuilder()
                .setDocumentId(documentId)
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("test-doc")
                        .setDocumentType("pdf")
                        .setContentType("application/pdf")
                        .setSize(1024L)
                        .setS3Key("test-key")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateConsistentKeysForDriveEvents() {
        // Given
        KafkaKeyStrategy keyStrategy = new KafkaKeyStrategy();
        DriveEvent event = DriveEvent.newBuilder()
                .setDriveId(12345L)
                .setCreated(DriveEvent.DriveCreated.newBuilder()
                        .setBucketName("test-bucket")
                        .setRegion("us-east-1")
                        .setDescription("Test drive")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isNotNull();
    }

    @Test
    void shouldGenerateDifferentKeysForDifferentDocuments() {
        // Given
        KafkaKeyStrategy keyStrategy = new KafkaKeyStrategy();
        DocumentEvent event1 = DocumentEvent.newBuilder()
                .setDocumentId("doc-111")
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("doc1")
                        .setDocumentType("pdf")
                        .build())
                .build();
                
        DocumentEvent event2 = DocumentEvent.newBuilder()
                .setDocumentId("doc-222")
                .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                        .setName("doc2")
                        .setDocumentType("pdf")
                        .build())
                .build();

        // When
        UUID key1 = keyStrategy.getKey(event1);
        UUID key2 = keyStrategy.getKey(event2);

        // Then
        assertThat(key1).isNotEqualTo(key2);
    }
}