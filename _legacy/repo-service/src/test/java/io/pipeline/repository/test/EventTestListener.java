package io.pipeline.repository.test;

import io.pipeline.repository.filesystem.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.jboss.logging.Logger;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test helper for listening to Kafka events and verifying they were sent.
 * Now uses the unified RepositoryEvent instead of separate event types.
 */
@ApplicationScoped
public class EventTestListener {

    private static final Logger LOG = Logger.getLogger(EventTestListener.class);

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    // Map of topic name to list of repository events
    private final Map<String, List<RepositoryEvent>> repositoryEvents = new ConcurrentHashMap<>();

    // Active consumers for cleanup
    private final Set<KafkaConsumer<UUID, ?>> activeConsumers = ConcurrentHashMap.newKeySet();

    /**
     * Start listening for RepositoryEvents on a specific topic.
     */
    public void startRepositoryEventListener(String topicName) {
        LOG.infof("Starting RepositoryEvent listener for topic: %s", topicName);

        repositoryEvents.put(topicName, new CopyOnWriteArrayList<>());

        CompletableFuture.runAsync(() -> {
            Properties props = createConsumerProperties("repo-listener-" + System.currentTimeMillis());

            try (KafkaConsumer<UUID, RepositoryEvent> consumer = new KafkaConsumer<>(props)) {
                activeConsumers.add(consumer);
                consumer.subscribe(Collections.singletonList(topicName));

                LOG.infof("Consumer subscribed to topic: %s", topicName);

                while (!Thread.currentThread().isInterrupted()) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<UUID, RepositoryEvent> record : records) {
                        RepositoryEvent event = record.value();
                        repositoryEvents.get(topicName).add(event);
                        LOG.infof("Received RepositoryEvent on topic %s: eventId=%s, documentId=%s, operation=%s",
                            topicName, event.getEventId(), event.getDocumentId(),
                            event.hasCreated() ? "created" : event.hasUpdated() ? "updated" : "deleted");
                    }
                }
            } catch (Exception e) {
                LOG.errorf(e, "Error in repository event listener for topic %s", topicName);
            } finally {
                LOG.infof("Repository event listener stopped for topic: %s", topicName);
            }
        });
    }

    /**
     * Wait for a specific number of repository events with timeout.
     */
    public List<RepositoryEvent> waitForRepositoryEvents(String topicName, int expectedCount, Duration timeout) {
        LOG.infof("Waiting for %d repository events on topic %s (timeout: %s)",
            expectedCount, topicName, timeout);

        if (!repositoryEvents.containsKey(topicName)) {
            startRepositoryEventListener(topicName);
            // Give the listener time to start
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        try {
            Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    List<RepositoryEvent> events = repositoryEvents.get(topicName);
                    if (events == null) {
                        return false;
                    }
                    LOG.debugf("Topic %s has %d events (waiting for %d)",
                        topicName, events.size(), expectedCount);
                    return events.size() >= expectedCount;
                });

            List<RepositoryEvent> events = repositoryEvents.get(topicName);
            LOG.infof("✅ Received %d repository events on topic %s", events.size(), topicName);
            return new ArrayList<>(events.subList(0, Math.min(expectedCount, events.size())));

        } catch (Exception e) {
            List<RepositoryEvent> events = repositoryEvents.get(topicName);
            int actualCount = events != null ? events.size() : 0;
            LOG.errorf("Timeout waiting for repository events. Expected: %d, Actual: %d on topic %s",
                expectedCount, actualCount, topicName);
            throw new AssertionError(String.format(
                "Timeout waiting for %d repository events on topic %s. Only received %d events",
                expectedCount, topicName, actualCount), e);
        }
    }

    /**
     * Get all current repository events for a topic.
     */
    public List<RepositoryEvent> getCurrentRepositoryEvents(String topicName) {
        List<RepositoryEvent> events = repositoryEvents.get(topicName);
        return events != null ? new ArrayList<>(events) : Collections.emptyList();
    }

    /**
     * Clear all events for a specific topic.
     */
    public void clearEvents(String topicName) {
        LOG.infof("Clearing events for topic: %s", topicName);
        List<RepositoryEvent> events = repositoryEvents.get(topicName);
        if (events != null) {
            events.clear();
        }
    }

    /**
     * Clear all events across all topics.
     */
    public void clearAllEvents() {
        LOG.info("Clearing all events from all topics");
        repositoryEvents.values().forEach(List::clear);
    }

    /**
     * Clear events for topics matching a prefix.
     */
    public void clearTopicsWithPrefix(String prefix) {
        LOG.infof("Clearing events for topics with prefix: %s", prefix);
        repositoryEvents.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .forEach(entry -> entry.getValue().clear());
    }

    /**
     * Stop all listeners and clean up resources.
     */
    public void stopAllListeners() {
        LOG.info("Stopping all event listeners");
        activeConsumers.forEach(consumer -> {
            try {
                consumer.wakeup();
                consumer.close();
            } catch (Exception e) {
                LOG.warnf(e, "Error closing consumer");
            }
        });
        activeConsumers.clear();
        repositoryEvents.clear();
    }

    private Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // Apicurio Registry configuration - use test port 8082
        props.put("apicurio.registry.url", "http://localhost:8082/apis/registry/v3");
        // Critical property for proper deserialization - matches opensearch-manager pattern
        props.put("apicurio.registry.serde.find-latest", "true");
        props.put("apicurio.registry.deserializer.value.return-class", RepositoryEvent.class.getName());
        return props;
    }

    // Legacy method signatures for backward compatibility with tests
    @Deprecated
    public void startDocumentEventListener(String topicName) {
        startRepositoryEventListener(topicName);
    }

    @Deprecated
    public List<RepositoryEvent> waitForDocumentEvents(String topicName, int expectedCount, Duration timeout) {
        return waitForRepositoryEvents(topicName, expectedCount, timeout);
    }

    @Deprecated
    public List<RepositoryEvent> getCurrentDocumentEvents(String topicName) {
        return getCurrentRepositoryEvents(topicName);
    }

    // Stub methods for removed event types
    @Deprecated
    public List<Object> waitForSearchIndexEvents(String topicName, int expectedCount, Duration timeout) {
        LOG.warn("SearchIndexEvents are no longer used - returning empty list");
        return Collections.emptyList();
    }

    @Deprecated
    public List<Object> getCurrentSearchIndexEvents(String topicName) {
        LOG.warn("SearchIndexEvents are no longer used - returning empty list");
        return Collections.emptyList();
    }

    @Deprecated
    public List<Object> waitForRequestCountEvents(String topicName, int expectedCount, Duration timeout) {
        LOG.warn("RequestCountEvents are no longer used - returning empty list");
        return Collections.emptyList();
    }

    @Deprecated
    public List<Object> getCurrentRequestCountEvents(String topicName) {
        LOG.warn("RequestCountEvents are no longer used - returning empty list");
        return Collections.emptyList();
    }
}