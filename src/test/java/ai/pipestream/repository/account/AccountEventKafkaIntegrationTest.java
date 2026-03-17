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

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that produces an AccountEvent through Kafka
 * and verifies the consumer (AccountCacheService) picks it up
 * and invalidates the cache entry.
 */
@QuarkusTest
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class AccountEventKafkaIntegrationTest {

    @Inject
    AccountCacheService accountCacheService;

    @Test
    void accountCreatedEventTriggersKafkaConsumer() throws Exception {
        // Use "valid-account" which WireMock knows about and returns active=true
        String accountId = "valid-account";

        // Prime the cache with a lookup
        boolean initiallyValid = accountCacheService.isValidAccount(accountId).await().indefinitely();
        assertTrue(initiallyValid, "valid-account should be active via WireMock");

        // Get the Kafka bootstrap servers from the running config
        String bootstrapServers = ConfigProvider.getConfig()
                .getValue("kafka.bootstrap.servers", String.class);
        String apicurioUrl = ConfigProvider.getConfig()
                .getValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class);

        // Build an AccountEvent (CREATED triggers drive auto-creation + cache invalidation)
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setCreated(AccountEvent.Created.newBuilder()
                        .setName("Kafka Integration Test Account")
                        .setDescription("Testing Kafka roundtrip")
                        .build())
                .build();

        // Produce it to Kafka
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
        }

        // Wait for consumer to process — the event invalidates cache and the
        // next isValidAccount call re-fetches from gRPC (WireMock returns active=true)
        Thread.sleep(2000);
        boolean stillValid = accountCacheService.isValidAccount(accountId).await().indefinitely();
        assertTrue(stillValid, "Account should still be valid after cache invalidation + re-fetch");
    }
}
