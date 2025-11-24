package ai.pipestream.repository.grpc;

import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.filesystem.CreateDriveRequest;
import ai.pipestream.repository.filesystem.RepositoryEvent;
import ai.pipestream.repository.pipedoc.SavePipeDocRequest;
import ai.pipestream.repository.pipedoc.SavePipeDocResponse;
import ai.pipestream.repository.service.DocumentStorageService;
import com.google.protobuf.ByteString;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test for RepositoryGrpcService following platform Kafka/Apicurio standards.
 * This test verifies that the producer correctly emits events to Kafka.
 */
@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
class RepositoryGrpcServiceTest {

    @Inject
    @io.quarkus.grpc.GrpcService
    RepositoryGrpcService repositoryGrpcService;

    @Inject
    @io.quarkus.grpc.GrpcService
    ai.pipestream.repository.grpc.DriveGrpcService driveGrpcService;

    @InjectMock
    DocumentStorageService storageService;

    // Inject config properties from PipelineKafkaConfigSource (connector-level)
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String apicurioRegistryUrl;

    /**
     * Creates a Kafka consumer for testing the producer.
     * Follows platform standard: UUID key + Protobuf value with Apicurio.
     */
    private KafkaConsumer<UUID, RepositoryEvent> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Apicurio specific properties
        props.put("apicurio.registry.url", apicurioRegistryUrl);
        props.put("apicurio.registry.deserializer.value.return-class", RepositoryEvent.class.getName());

        return new KafkaConsumer<>(props);
    }

    @Test
    void testSavePipeDocEmitsEvent() {
        // Mock the storage service
        when(storageService.uploadPipeDoc(any(String.class), any(String.class), any(PipeDoc.class)))
                .thenReturn(Uni.createFrom().item("etag-123"));

        // Create a test Drive first
        String driveName = "test-drive-" + UUID.randomUUID();
        String bucketName = "test-bucket-" + UUID.randomUUID();
        String accountId = "test-account-" + UUID.randomUUID();

        CreateDriveRequest createDriveRequest = CreateDriveRequest.newBuilder()
                .setName(driveName)
                .setBucketName(bucketName)
                .setAccountId(accountId)
                .build();

        driveGrpcService.createDrive(createDriveRequest).await().indefinitely();

        // Create a test PipeDoc
        String docId = UUID.randomUUID().toString();
        String connectorId = "test-connector";
        String content = "test content";
        BlobBag.Builder blobBag = BlobBag.newBuilder();
        Blob blog = Blob.newBuilder()
                .setBlobId("blob-1")
                .setSizeBytes(content.getBytes().length)
                .setChecksum("blob-checksum-1")
                .build();
        blobBag.setBlob(blog);

        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setBlobBag(blobBag.build())
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(pipeDoc)
                .setDrive(driveName)
                .setConnectorId(connectorId)
                .build();

        // Call the service
        SavePipeDocResponse response = repositoryGrpcService.savePipeDoc(request).await().indefinitely();

        // Verify response
        assertNotNull(response);
        assertEquals(docId, response.getNodeId());
        assertEquals(driveName, response.getDrive());
        assertEquals("etag-123", response.getChecksum());

        // Verify Kafka event
        try (KafkaConsumer<UUID, RepositoryEvent> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("repository-events"));

            // ASSERT: Use Awaitility to poll for the Kafka event
            RepositoryEvent foundEvent = Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .until(() -> pollForMessage(consumer, docId), Matchers.notNullValue());

            // Verify the event content
            assertNotNull(foundEvent);
            assertTrue(foundEvent.hasCreated(), "Event should be a Created event");
            assertEquals(docId, foundEvent.getDocumentId());
            assertEquals("etag-123", foundEvent.getCreated().getContentHash());
            assertEquals(bucketName, foundEvent.getCreated().getBucketName());
            assertEquals(connectorId, foundEvent.getSource().getConnectorId());
            assertEquals(accountId, foundEvent.getAccountId());
        }
    }

    /**
     * Helper method for Awaitility polling.
     * Returns the event if found, null otherwise.
     */
    private RepositoryEvent pollForMessage(KafkaConsumer<UUID, RepositoryEvent> consumer, String docId) {
        ConsumerRecords<UUID, RepositoryEvent> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<UUID, RepositoryEvent> record : records) {
            if (record.value().getDocumentId().equals(docId)) {
                return record.value();
            }
        }
        return null;
    }
}
