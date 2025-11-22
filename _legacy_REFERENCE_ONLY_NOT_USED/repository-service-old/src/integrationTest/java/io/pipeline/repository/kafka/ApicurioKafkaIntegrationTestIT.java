package io.pipeline.repository.kafka;

import io.pipeline.grpc.wiremock.WireMockGrpcTestResource;
import io.pipeline.grpc.wiremock.PlatformRegistrationMock;
import io.pipeline.grpc.wiremock.InjectWireMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Apicurio Kafka event publishing.
 * 
 * This test validates the complete end-to-end flow:
 * - MutinyEmitter + Apicurio Registry v3 integration
 * - Proper UUID key generation and Kafka partitioning
 * - Event publishing through RepoEmitter abstraction
 * - Message consumption via Kafka topics
 * - WireMock integration for Platform Registration Service
 * 
 * Uses @QuarkusIntegrationTest to start the full application with DevServices.
 * Tests the actual Kafka/Apicurio integration without injection.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(WireMockGrpcTestResource.class)
public class ApicurioKafkaIntegrationTestIT {

    @InjectWireMock
    com.github.tomakehurst.wiremock.WireMockServer wireMockServer;

    private HttpClient httpClient;
    private String baseUrl;

    @BeforeEach
    void setUp() {
        // Clear any previous test data
        IntegrationTestKafkaConsumer.clearAllMessages();
        
        // Set up WireMock stubs for Platform Registration Service
        if (wireMockServer != null) {
            PlatformRegistrationMock mock = new PlatformRegistrationMock(wireMockServer.port());
            mock.mockServiceRegistration()
                .mockModuleRegistration();
        }
        
        // Set up HTTP client for calling test endpoints
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        
        // Get the application URL (QuarkusIntegrationTest provides this)
        baseUrl = "http://localhost:8080";
    }

    @Test
    void shouldPublishAndConsumeDriveEvents() throws Exception {
        // Given
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/drive-event"))
                .GET()
                .build();

        // When
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\": \"success\"");
        assertThat(response.body()).contains("\"driveId\": 12345");
        
        // Wait for message to be consumed
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> IntegrationTestKafkaConsumer.getDriveEvents().size() >= 1);
        
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.DriveEvent>> consumedEvents = 
                IntegrationTestKafkaConsumer.getDriveEvents();
        
        assertThat(consumedEvents).hasSize(1);
        
        io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.DriveEvent> consumedEvent = consumedEvents.get(0);
        assertThat(consumedEvent.getKey()).isNotNull();
        assertThat(consumedEvent.getPayload()).isNotNull();
        assertThat(consumedEvent.getPayload().getDriveId()).isEqualTo(12345L);
        assertThat(consumedEvent.getPayload().getDriveName()).isEqualTo("test-drive");
    }

    @Test
    void shouldPublishAndConsumeDocumentEvents() throws Exception {
        // Given
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/document-event"))
                .GET()
                .build();

        // When
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\": \"success\"");
        assertThat(response.body()).contains("\"documentId\": \"doc-integration-test-123\"");
        
        // Wait for message to be consumed
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> IntegrationTestKafkaConsumer.getDocumentEvents().size() >= 1);
        
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.DocumentEvent>> consumedEvents = 
                IntegrationTestKafkaConsumer.getDocumentEvents();
        
        assertThat(consumedEvents).hasSize(1);
        
        io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.DocumentEvent> consumedEvent = consumedEvents.get(0);
        assertThat(consumedEvent.getKey()).isNotNull();
        assertThat(consumedEvent.getPayload()).isNotNull();
        assertThat(consumedEvent.getPayload().getDocumentId()).isEqualTo("doc-integration-test-123");
        assertThat(consumedEvent.getPayload().getDriveId()).isEqualTo(12345L);
        assertThat(consumedEvent.getPayload().getPath()).isEqualTo("/test/test-document.pdf");
    }

    @Test
    void shouldPublishAndConsumeSearchIndexEvents() throws Exception {
        // Given
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/search-index-event"))
                .GET()
                .build();

        // When
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\": \"success\"");
        assertThat(response.body()).contains("\"documentId\": \"doc-integration-test-123\"");
        
        // Wait for message to be consumed
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> IntegrationTestKafkaConsumer.getSearchIndexEvents().size() >= 1);
        
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.SearchIndexEvent>> consumedEvents = 
                IntegrationTestKafkaConsumer.getSearchIndexEvents();
        
        assertThat(consumedEvents).hasSize(1);
        
        io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.SearchIndexEvent> consumedEvent = consumedEvents.get(0);
        assertThat(consumedEvent.getKey()).isNotNull();
        assertThat(consumedEvent.getPayload()).isNotNull();
        assertThat(consumedEvent.getPayload().getDocumentId()).isEqualTo("doc-integration-test-123");
        assertThat(consumedEvent.getPayload().getIndexName()).isEqualTo("documents");
    }

    @Test
    void shouldPublishAndConsumeRequestCountEvents() throws Exception {
        // Given
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/request-count-event"))
                .GET()
                .build();

        // When
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\": \"success\"");
        assertThat(response.body()).contains("\"documentId\": \"doc-integration-test-123\"");
        
        // Wait for message to be consumed
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> IntegrationTestKafkaConsumer.getRequestCountEvents().size() >= 1);
        
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.RequestCountEvent>> consumedEvents = 
                IntegrationTestKafkaConsumer.getRequestCountEvents();
        
        assertThat(consumedEvents).hasSize(1);
        
        io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.RequestCountEvent> consumedEvent = consumedEvents.get(0);
        assertThat(consumedEvent.getKey()).isNotNull();
        assertThat(consumedEvent.getPayload()).isNotNull();
        assertThat(consumedEvent.getPayload().getDriveId()).isEqualTo(12345L);
        assertThat(consumedEvent.getPayload().hasDocumentRequest()).isTrue();
        assertThat(consumedEvent.getPayload().getDocumentRequest().getDocumentId()).isEqualTo("doc-integration-test-123");
        assertThat(consumedEvent.getPayload().getDocumentRequest().getOperation()).isEqualTo("READ");
    }

    @Test
    void shouldMaintainConsistentPartitioningForSameDocument() throws Exception {
        // Given - Send multiple events for the same document
        HttpRequest docRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/document-event"))
                .GET()
                .build();
                
        HttpRequest searchRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/search-index-event"))
                .GET()
                .build();
                
        HttpRequest countRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/request-count-event"))
                .GET()
                .build();

        // When
        httpClient.send(docRequest, HttpResponse.BodyHandlers.ofString());
        httpClient.send(searchRequest, HttpResponse.BodyHandlers.ofString());
        httpClient.send(countRequest, HttpResponse.BodyHandlers.ofString());

        // Then
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .until(() -> {
                    int totalMessages = IntegrationTestKafkaConsumer.getDocumentEvents().size() +
                                      IntegrationTestKafkaConsumer.getSearchIndexEvents().size() +
                                      IntegrationTestKafkaConsumer.getRequestCountEvents().size();
                    return totalMessages >= 3;
                });
        
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.DocumentEvent>> docEvents = 
                IntegrationTestKafkaConsumer.getDocumentEvents();
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.SearchIndexEvent>> searchEvents = 
                IntegrationTestKafkaConsumer.getSearchIndexEvents();
        List<io.smallrye.reactive.messaging.kafka.KafkaRecord<UUID, io.pipeline.repository.filesystem.RequestCountEvent>> requestEvents = 
                IntegrationTestKafkaConsumer.getRequestCountEvents();
        
        assertThat(docEvents).hasSize(1);
        assertThat(searchEvents).hasSize(1);
        assertThat(requestEvents).hasSize(1);
        
        // All events for the same document should have the same key (same partition)
        UUID docKey = docEvents.get(0).getKey();
        UUID searchKey = searchEvents.get(0).getKey();
        UUID requestKey = requestEvents.get(0).getKey();
        
        assertThat(docKey).isEqualTo(searchKey);
        assertThat(docKey).isEqualTo(requestKey);
        assertThat(searchKey).isEqualTo(requestKey);
    }

    @Test
    void shouldPublishAllEventsSuccessfully() throws Exception {
        // Given
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/test/kafka/all-events"))
                .GET()
                .build();

        // When
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // Then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\": \"success\"");
        assertThat(response.body()).contains("\"message\": \"All events sent\"");
        
        // Wait for all messages to be consumed
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .until(() -> IntegrationTestKafkaConsumer.getTotalMessageCount() >= 4);
        
        assertThat(IntegrationTestKafkaConsumer.getDriveEvents()).hasSize(1);
        assertThat(IntegrationTestKafkaConsumer.getDocumentEvents()).hasSize(1);
        assertThat(IntegrationTestKafkaConsumer.getSearchIndexEvents()).hasSize(1);
        assertThat(IntegrationTestKafkaConsumer.getRequestCountEvents()).hasSize(1);
    }
}