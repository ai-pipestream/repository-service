package io.pipeline.repository.test;

import io.pipeline.repository.filesystem.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.jboss.logging.Logger;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Test consumer for RepositoryEvents.
 * Updated to use the new RepositoryEvent structure.
 */
public class TestEventConsumer {
    
    private static final Logger LOG = Logger.getLogger(TestEventConsumer.class);
    
    /**
     * Wait for RepositoryEvents on a specific topic.
     */
    public List<RepositoryEvent> waitForRepositoryEvents(String topicName, int expectedCount, Duration timeout) {
        LOG.infof("Waiting for %d RepositoryEvents on topic: %s", expectedCount, topicName);
        
        List<RepositoryEvent> receivedEvents = new ArrayList<>();
        
        Properties props = createConsumerProperties("test-consumer-" + System.currentTimeMillis());
        
        try (KafkaConsumer<UUID, RepositoryEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            
            long startTime = System.currentTimeMillis();
            long timeoutMs = timeout.toMillis();
            
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                var records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<UUID, RepositoryEvent> record : records) {
                    RepositoryEvent event = record.value();
                    LOG.infof("Received RepositoryEvent: documentId=%s", event.getDocumentId());
                    receivedEvents.add(event);
                    
                    if (receivedEvents.size() >= expectedCount) {
                        LOG.infof("Received expected number of events: %d", receivedEvents.size());
                        return receivedEvents;
                    }
                }
            }
            
            LOG.warnf("Timeout waiting for events. Expected: %d, Received: %d", expectedCount, receivedEvents.size());
            return receivedEvents;
            
        } catch (Exception e) {
            LOG.errorf(e, "Error consuming RepositoryEvents from topic: %s", topicName);
            throw new RuntimeException("Failed to consume events", e);
        }
    }
    
    /**
     * Wait for RepositoryEvents asynchronously.
     */
    public CompletableFuture<List<RepositoryEvent>> waitForRepositoryEventsAsync(String topicName, int expectedCount, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> waitForRepositoryEvents(topicName, expectedCount, timeout));
    }
    
    /**
     * Create consumer properties for RepositoryEvent deserialization.
     */
    private Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        // Apicurio Registry configuration
        props.put("apicurio.registry.url", "http://localhost:8081/apis/registry/v2");
        props.put("apicurio.registry.auto-register", "true");
        props.put("apicurio.registry.find-latest", "true");
        
        return props;
    }
    
    // Legacy method names for backward compatibility
    public List<RepositoryEvent> waitForDocumentEvents(String topicName, int expectedCount, Duration timeout) {
        return waitForRepositoryEvents(topicName, expectedCount, timeout);
    }
    
    public CompletableFuture<List<RepositoryEvent>> waitForDocumentEventsAsync(String topicName, int expectedCount, Duration timeout) {
        return waitForRepositoryEventsAsync(topicName, expectedCount, timeout);
    }
}

