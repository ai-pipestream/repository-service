package io.pipeline.repository.kafka;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for KafkaKeyStrategy.
 * 
 * Tests the document-centric partitioning strategy:
 * - Document-related events use documentId (PipeDoc ID)
 * - Drive events use driveId
 * - Request count events use documentId when available, fallback to driveId
 * - Same entity ID always generates the same UUID key
 */
class KafkaKeyStrategyTest {

    private KafkaKeyStrategy keyStrategy;

    @BeforeEach
    void setUp() {
        keyStrategy = new KafkaKeyStrategy();
    }

    @Test
    @DisplayName("Should generate consistent UUID keys for DriveEvent using driveId")
    void shouldGenerateConsistentKeysForDriveEvent() {
        // Given: A DriveEvent with specific driveId
        DriveEvent event = DriveEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .build();

        // When: Generating keys multiple times
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then: Keys should be identical and deterministic
        assertThat(key1)
            .as("Generated keys should be identical for the same driveId")
            .isEqualTo(key2);
        
        assertThat(key1)
            .as("Generated key should not be null")
            .isNotNull();
        
        // Verify different driveId generates different key
        DriveEvent differentEvent = DriveEvent.newBuilder()
            .setDriveId(67890L)
            .setDriveName("different-drive")
            .setCustomerId("customer-456")
            .build();
        
        UUID differentKey = keyStrategy.getKey(differentEvent);
        
        assertThat(differentKey)
            .as("Different driveId should generate different key")
            .isNotEqualTo(key1);
    }

    @Test
    @DisplayName("Should generate consistent UUID keys for DocumentEvent using documentId")
    void shouldGenerateConsistentKeysForDocumentEvent() {
        // Given: A DocumentEvent with specific documentId (PipeDoc ID)
        DocumentEvent event = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .build();

        // When: Generating keys multiple times
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then: Keys should be identical and deterministic
        assertThat(key1)
            .as("Generated keys should be identical for the same documentId")
            .isEqualTo(key2);
        
        assertThat(key1)
            .as("Generated key should not be null")
            .isNotNull();
        
        // Verify different documentId generates different key
        DocumentEvent differentEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-456")
            .setDriveId(12345L) // Same drive, different document
            .setDriveName("test-drive")
            .setPath("/test/path/other-document.pdf")
            .build();
        
        UUID differentKey = keyStrategy.getKey(differentEvent);
        
        assertThat(differentKey)
            .as("Different documentId should generate different key")
            .isNotEqualTo(key1);
    }

    @Test
    @DisplayName("Should generate consistent UUID keys for SearchIndexEvent using documentId")
    void shouldGenerateConsistentKeysForSearchIndexEvent() {
        // Given: A SearchIndexEvent with specific documentId
        SearchIndexEvent event = SearchIndexEvent.newBuilder()
            .setDocumentId("pipe-doc-123")
            .setDriveName("test-drive")
            .setIndexName("search-index")
            .build();

        // When: Generating keys multiple times
        UUID key1 = keyStrategy.getKey(event);
        UUID key2 = keyStrategy.getKey(event);

        // Then: Keys should be identical and deterministic
        assertThat(key1)
            .as("Generated keys should be identical for the same documentId")
            .isEqualTo(key2);
        
        assertThat(key1)
            .as("Generated key should not be null")
            .isNotNull();
    }

    @Test
    @DisplayName("Should generate same UUID key for DocumentEvent and SearchIndexEvent with same documentId")
    void shouldGenerateSameKeyForDocumentAndSearchEventsWithSameDocumentId() {
        // Given: DocumentEvent and SearchIndexEvent with same documentId
        String documentId = "pipe-doc-123";
        
        DocumentEvent docEvent = DocumentEvent.newBuilder()
            .setDocumentId(documentId)
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .build();
        
        SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
            .setDocumentId(documentId)
            .setDriveName("test-drive")
            .setIndexName("search-index")
            .build();

        // When: Generating keys for both events
        UUID docKey = keyStrategy.getKey(docEvent);
        UUID searchKey = keyStrategy.getKey(searchEvent);

        // Then: Keys should be identical (same partition for related events)
        assertThat(docKey)
            .as("DocumentEvent and SearchIndexEvent with same documentId should generate identical keys")
            .isEqualTo(searchKey);
    }

    @Test
    @DisplayName("Should generate UUID key for RequestCountEvent using documentId when document request is available")
    void shouldGenerateKeyForRequestCountEventUsingDocumentId() {
        // Given: RequestCountEvent with document request
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDocumentRequest(RequestCountEvent.DocumentRequestCount.newBuilder()
                .setDocumentId("pipe-doc-123")
                .setOperation("READ")
                .setBytesTransferred(1024L)
                .setProcessingTimeMs(150L)
                .build())
            .build();

        // When: Generating key
        UUID key = keyStrategy.getKey(event);

        // Then: Key should be generated using documentId
        assertThat(key)
            .as("Generated key should not be null")
            .isNotNull();
        
        // Verify it generates the same key as DocumentEvent with same documentId
        DocumentEvent docEvent = DocumentEvent.newBuilder()
            .setDocumentId("pipe-doc-123")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .build();
        
        UUID docKey = keyStrategy.getKey(docEvent);
        
        assertThat(key)
            .as("RequestCountEvent with document request should generate same key as DocumentEvent with same documentId")
            .isEqualTo(docKey);
    }

    @Test
    @DisplayName("Should generate UUID key for RequestCountEvent using driveId when no document request")
    void shouldGenerateKeyForRequestCountEventUsingDriveId() {
        // Given: RequestCountEvent with drive request (no document request)
        RequestCountEvent event = RequestCountEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .setDriveRequest(RequestCountEvent.DriveRequestCount.newBuilder()
                .setOperation("CREATE")
                .setProcessingTimeMs(200L)
                .build())
            .build();

        // When: Generating key
        UUID key = keyStrategy.getKey(event);

        // Then: Key should be generated using driveId
        assertThat(key)
            .as("Generated key should not be null")
            .isNotNull();
        
        // Verify it generates the same key as DriveEvent with same driveId
        DriveEvent driveEvent = DriveEvent.newBuilder()
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setCustomerId("customer-123")
            .build();
        
        UUID driveKey = keyStrategy.getKey(driveEvent);
        
        assertThat(key)
            .as("RequestCountEvent with drive request should generate same key as DriveEvent with same driveId")
            .isEqualTo(driveKey);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for unknown message type")
    void shouldThrowExceptionForUnknownMessageType() {
        // Given: An unknown message type
        String unknownMessage = "not-a-protobuf-message";

        // When/Then: Should throw IllegalArgumentException
        assertThatThrownBy(() -> keyStrategy.getKey(unknownMessage))
            .as("Should throw IllegalArgumentException for unknown message type")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should generate deterministic UUIDs using nameUUIDFromBytes")
    void shouldGenerateDeterministicUuids() {
        // Given: Same input string
        String input = "pipe-doc-123";
        
        // When: Generating UUIDs multiple times
        UUID uuid1 = UUID.nameUUIDFromBytes(input.getBytes());
        UUID uuid2 = UUID.nameUUIDFromBytes(input.getBytes());
        
        // Then: Should be identical
        assertThat(uuid1)
            .as("UUID.nameUUIDFromBytes should generate identical UUIDs for same input")
            .isEqualTo(uuid2);
        
        // Verify it's deterministic across different inputs
        String differentInput = "pipe-doc-456";
        UUID differentUuid = UUID.nameUUIDFromBytes(differentInput.getBytes());
        
        assertThat(differentUuid)
            .as("Different inputs should generate different UUIDs")
            .isNotEqualTo(uuid1);
    }

    @Test
    @DisplayName("Should handle edge cases for key generation")
    void shouldHandleEdgeCasesForKeyGeneration() {
        // Test with empty documentId
        DocumentEvent emptyDocEvent = DocumentEvent.newBuilder()
            .setDocumentId("")
            .setDriveId(12345L)
            .setDriveName("test-drive")
            .setPath("/test/path/document.pdf")
            .build();
        
        UUID emptyKey = keyStrategy.getKey(emptyDocEvent);
        assertThat(emptyKey)
            .as("Should handle empty documentId")
            .isNotNull();
        
        // Test with zero driveId
        DriveEvent zeroDriveEvent = DriveEvent.newBuilder()
            .setDriveId(0L)
            .setDriveName("zero-drive")
            .setCustomerId("customer-zero")
            .build();
        
        UUID zeroKey = keyStrategy.getKey(zeroDriveEvent);
        assertThat(zeroKey)
            .as("Should handle zero driveId")
            .isNotNull();
        
        // Verify different edge cases generate different keys
        assertThat(emptyKey)
            .as("Empty documentId and zero driveId should generate different keys")
            .isNotEqualTo(zeroKey);
    }
}