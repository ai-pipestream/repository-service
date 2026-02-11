package ai.pipestream.repository.account;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that produces an AccountEvent through Kafka
 * and verifies the consumer (AccountCacheService) picks it up.
 */
@QuarkusTest
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class AccountEventKafkaIntegrationTest {

    @Inject
    AccountCacheService accountCacheService;

    @Test
    void accountEventProducedToKafkaIsConsumedByCache() throws Exception {
        String accountId = "kafka-test-" + UUID.randomUUID();

        // Verify account is NOT in cache initially
        boolean initiallyValid = accountCacheService.isValidAccount(accountId).await().indefinitely();
        assert !initiallyValid : "Account should not be in cache initially";

        // Get the Kafka bootstrap servers from the running config
        String bootstrapServers = ConfigProvider.getConfig()
                .getValue("kafka.bootstrap.servers", String.class);
        String apicurioUrl = ConfigProvider.getConfig()
                .getValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class);

        System.out.println("=== Kafka bootstrap: " + bootstrapServers);
        System.out.println("=== Apicurio URL: " + apicurioUrl);

        // Build an AccountEvent
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setCreated(AccountEvent.Created.newBuilder()
                        .setName("Kafka Integration Test Account")
                        .setDescription("Testing Kafka roundtrip")
                        .build())
                .build();

        // Produce it to Kafka using the Apicurio protobuf serializer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.UUIDSerializer");
        props.put("value.serializer", "io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer");
        props.put("apicurio.registry.url", apicurioUrl);
        props.put("apicurio.registry.auto-register", "true");
        props.put("apicurio.registry.artifact-resolver-strategy",
                "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");

        try (KafkaProducer<UUID, AccountEvent> producer = new KafkaProducer<>(props)) {
            ProducerRecord<UUID, AccountEvent> record = new ProducerRecord<>(
                    "account-events",
                    UUID.nameUUIDFromBytes(accountId.getBytes()),
                    event);
            producer.send(record).get(10, TimeUnit.SECONDS);
            System.out.println("=== Successfully produced AccountEvent to Kafka for account: " + accountId);
        }

        // Wait for the consumer to pick it up (poll with timeout)
        boolean found = false;
        for (int i = 0; i < 30; i++) {
            Thread.sleep(500);
            boolean valid = accountCacheService.isValidAccount(accountId).await().indefinitely();
            if (valid) {
                found = true;
                System.out.println("=== Consumer picked up event after " + ((i + 1) * 500) + "ms");
                break;
            }
        }

        assertTrue(found, "AccountCacheService should have received the event from Kafka within 15 seconds");
    }
}
