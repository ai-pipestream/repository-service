# Kafka Event Patterns

## Overview

The repository service publishes events to Kafka for:
1. **State changes** - Document lifecycle events (created, updated, deleted)
2. **Async processing** - Trigger downstream services (indexing, parsing, analytics)
3. **Audit trail** - Track who did what when
4. **Loose coupling** - Services don't need direct dependencies

## Event Design Principles

### 1. Lean Events (State Changes Only)

Events contain **minimal information** - just enough to identify what changed:

```protobuf
message RepositoryEvent {
  string event_id = 1;               // Deterministic: hash(document_id + operation + timestamp)
  google.protobuf.Timestamp timestamp = 2;
  string document_id = 3;            // Client-provided stable ID
  string account_id = 4;             // From Drive entity

  SourceContext source = 5;          // Who/what/where initiated this

  oneof operation {
    Created created = 10;
    Updated updated = 11;
    Deleted deleted = 12;
  }
}
```

**Why lean events?**
- Kafka message size limits (1MB default)
- Events are immutable (full payload may change, but state transition doesn't)
- Consumers fetch latest state from repository service (callback pattern)
- Reduces event schema brittleness

### 2. Deterministic Event IDs

Event IDs are **deterministic hashes** to enable idempotent processing:

```java
@ApplicationScoped
public class EventIdGenerator {

    /**
     * Generate deterministic event ID for idempotency.
     * Same inputs always produce the same event ID, enabling deduplication.
     *
     * @param documentId The document UUID
     * @param operation The operation type (e.g., "created", "updated")
     * @param timestampMs The event timestamp in milliseconds
     * @return A deterministic 16-character event ID
     */
    public String generateEventId(String documentId, String operation, long timestampMs) {
        // Concatenate inputs to form a unique string
        String data = String.format("%s:%s:%d", documentId, operation, timestampMs);

        try {
            // Hash using SHA-256
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));

            // Convert to hex and take first 16 characters
            StringBuilder hexString = new StringBuilder();
            for (int i = 0; i < Math.min(8, hash.length); i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}

// Example usage:
// EventIdGenerator generator = new EventIdGenerator();
// String eventId = generator.generateEventId(
//     "550e8400-e29b-41d4-a716-446655440000",
//     "created",
//     1700000000000L
// );
// Result: "a1b2c3d4e5f67890"
```

**Benefits:**
- Consumers can deduplicate events (store event_id in processed set)
- Retry-safe: Same event processed twice = same event_id
- Traceability: Event ID uniquely identifies state transition

### 3. Source Context for Audit Trail

Every event includes source context:

```protobuf
message SourceContext {
  string component = 1;          // REQUIRED: "repository-service", "connector-xyz"
  string operation = 2;          // REQUIRED: "create", "update", "delete"
  string request_id = 3;         // REQUIRED: For tracing (deterministic UUID)

  // Optional context when applicable:
  string user_id = 4;           // If user-initiated
  string connector_id = 5;      // If connector-initiated
  string session_id = 6;        // For grouping related operations
}
```

**Example:**
```java
// Building a SourceContext protobuf message in Java
SourceContext source = SourceContext.newBuilder()
    .setComponent("repository-service")
    .setOperation("create")
    .setRequestId(UUID.randomUUID().toString())
    .setConnectorId("filesystem-prod-01")
    .setSessionId("crawl-session-abc123")
    .build();
```

## Event Topics

### Topic Naming Convention

```
{service}.{entity}.{operation}

Examples:
  repository.documents.created
  repository.documents.updated
  repository.documents.deleted
  repository.uploads.initiated
  repository.uploads.completed
  repository.uploads.failed
```

### Topic: `repository.uploads.initiated`

Published when upload starts (Phase 1).

```json
{
  "event_id": "a1b2c3d4e5f67890",
  "timestamp": "2024-11-13T12:00:00.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "initiate_upload",
    "request_id": "req-abc123",
    "connector_id": "filesystem-prod"
  },
  "initiated": {
    "s3_key": "connectors/filesystem-prod/550e8400-e29b-41d4-a716-446655440000.pb",
    "expected_size": 1073741824,
    "mime_type": "application/pdf"
  }
}
```

**Consumers:**
- Analytics service: Track upload volume
- Monitoring: Alert if uploads spike

### Topic: `repository.uploads.completed`

Published when all chunks uploaded and S3 multipart completed.

```json
{
  "event_id": "b2c3d4e5f6789012",
  "timestamp": "2024-11-13T12:05:00.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "complete_upload",
    "request_id": "req-abc123",
    "connector_id": "filesystem-prod"
  },
  "created": {
    "s3_key": "connectors/filesystem-prod/550e8400-e29b-41d4-a716-446655440000.pb",
    "size": 1073741824,
    "content_hash": "sha256:abc123...",
    "s3_version_id": "v1.0",
    "s3_etag": "\"abc123...\""
  }
}
```

**Consumers:**
- **OpenSearch indexing service**: Index document metadata
- **Tika parser service**: Extract text and metadata
- **Notification service**: Alert user of successful upload
- **Analytics service**: Track storage usage

### Topic: `repository.uploads.failed`

Published when upload fails.

```json
{
  "event_id": "c3d4e5f678901234",
  "timestamp": "2024-11-13T12:03:00.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "upload",
    "request_id": "req-abc123",
    "connector_id": "filesystem-prod"
  },
  "failed": {
    "reason": "S3 upload timeout",
    "error_code": "UPLOAD_TIMEOUT",
    "retry_count": 3,
    "partial_chunks_uploaded": 5,
    "total_chunks": 10
  }
}
```

**Consumers:**
- Monitoring/alerting: Track failure rate
- Analytics: Identify problem connectors
- Notification service: Alert user of failure

### Topic: `repository.documents.created`

Published when document is fully created and active (after upload completes).

```json
{
  "event_id": "d4e5f67890123456",
  "timestamp": "2024-11-13T12:05:01.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "create",
    "request_id": "req-abc123",
    "connector_id": "filesystem-prod"
  },
  "created": {
    "s3_key": "connectors/filesystem-prod/550e8400-e29b-41d4-a716-446655440000.pb",
    "size": 1073741824,
    "content_hash": "sha256:abc123...",
    "s3_version_id": "v1.0"
  }
}
```

**Consumers:**
- **OpenSearch indexer**: Fetch full document metadata via gRPC callback
- **Graph service**: Update document graph
- **Cache invalidation**: Clear related caches

### Topic: `repository.documents.updated`

Published when document content or metadata changes.

```json
{
  "event_id": "e5f6789012345678",
  "timestamp": "2024-11-13T13:00:00.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "update",
    "request_id": "req-def456",
    "user_id": "user-789"
  },
  "updated": {
    "s3_key": "connectors/filesystem-prod/550e8400-e29b-41d4-a716-446655440000.pb",
    "size": 1073741890,
    "content_hash": "sha256:def456...",
    "s3_version_id": "v2.0",
    "previous_version_id": "v1.0"
  }
}
```

**Consumers:**
- OpenSearch indexer: Re-index updated document
- Cache invalidation: Evict cached copies
- Version control service: Track document history

### Topic: `repository.documents.deleted`

Published when document is deleted (soft delete).

```json
{
  "event_id": "f67890123456789a",
  "timestamp": "2024-11-13T14:00:00.000Z",
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "account-123",
  "source": {
    "component": "repository-service",
    "operation": "delete",
    "request_id": "req-ghi789",
    "user_id": "user-789"
  },
  "deleted": {
    "reason": "User requested deletion",
    "purged": false  // S3 object still exists (soft delete)
  }
}
```

**Consumers:**
- OpenSearch indexer: Remove from index
- Cache invalidation: Evict cached copies
- Audit service: Log deletion

## Callback Pattern for Consumers

**Problem:** Events are lean (no full payload), but consumers need full document data.

**Solution:** Consumers **call back** to repository service via gRPC to fetch latest state.

### Example: OpenSearch Indexer

```java
@ApplicationScoped
public class OpenSearchIndexer {

    private static final Logger logger = Logger.getLogger(OpenSearchIndexer.class);

    @Inject
    @GrpcClient("repository-service")
    ManagedChannel repositoryChannel;

    @Inject
    RedisClient redis;

    @Inject
    @RestClient
    OpenSearchClient openSearchClient;

    /**
     * Consume Kafka events for document creation and index them in OpenSearch.
     * Uses Quarkus reactive messaging for Kafka consumption.
     */
    @Incoming("repository-documents-created")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public CompletionStage<Void> consumeDocumentCreatedEvent(Message<byte[]> message) {
        // Parse protobuf event from Kafka message
        RepositoryEvent event;
        try {
            event = RepositoryEvent.parseFrom(message.getPayload());
        } catch (InvalidProtocolBufferException e) {
            logger.errorf("Failed to parse event: %s", e.getMessage());
            return CompletableFuture.completedFuture(null);
        }

        // Check if already processed (idempotency protection)
        // This prevents duplicate indexing if Kafka redelivers the message
        if (isEventProcessed(event.getEventId())) {
            logger.infof("Skipping already processed event: %s", event.getEventId());
            return CompletableFuture.completedFuture(null);
        }

        // Callback to repository service for full document metadata
        // Events are lean (just IDs), so we fetch full data via gRPC
        GetNodeResponse document = fetchDocument(event.getDocumentId());

        if (document != null) {
            // Index document in OpenSearch
            indexDocument(document);

            // Mark event as processed (stored in Redis for deduplication)
            markEventProcessed(event.getEventId());
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Fetch full document metadata via gRPC callback to repository service.
     * This is the "callback pattern" - events contain minimal data, consumers fetch full state.
     */
    private GetNodeResponse fetchDocument(String documentId) {
        try {
            // Create gRPC stub for repository service
            NodeServiceGrpc.NodeServiceBlockingStub stub =
                NodeServiceGrpc.newBlockingStub(repositoryChannel);

            // Request full document metadata (without payload data for indexing)
            GetNodeRequest request = GetNodeRequest.newBuilder()
                .setDocumentId(documentId)
                .setIncludePayload(false)  // Metadata only, not file content
                .build();

            return stub.getNode(request);

        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // Document was deleted between event and callback
                logger.warnf("Document %s not found (may have been deleted)", documentId);
                return null;
            }
            // Re-throw other errors (will trigger retry via Kafka)
            throw e;
        }
    }

    /**
     * Index document in OpenSearch.
     * Converts protobuf document to JSON for OpenSearch indexing.
     */
    private void indexDocument(GetNodeResponse document) {
        // Build OpenSearch document from protobuf response
        Map<String, Object> doc = Map.of(
            "name", document.getName(),
            "path", document.getPath(),
            "content_type", document.getContentType(),
            "size", document.getSizeBytes(),
            "created_at", document.getCreatedAt().getSeconds() * 1000,
            "updated_at", document.getUpdatedAt().getSeconds() * 1000,
            "metadata", parseMetadata(document.getMetadata()),
            "s3_key", document.getS3Key(),
            "account_id", document.getDrive().getAccountId()
        );

        // Index in OpenSearch
        openSearchClient.index(
            "documents",                      // index name
            document.getDocumentId(),         // document ID
            doc                                // document body
        );

        logger.infof("Indexed document %s in OpenSearch", document.getDocumentId());
    }

    /**
     * Check if event has already been processed.
     * Uses Redis set for fast lookup with 30-day retention.
     */
    private boolean isEventProcessed(String eventId) {
        return redis.sismember("processed_events", eventId)
            .toCompletableFuture().join();
    }

    /**
     * Mark event as processed in Redis.
     * Prevents duplicate processing if Kafka redelivers messages.
     */
    private void markEventProcessed(String eventId) {
        // Add to Redis set
        redis.sadd("processed_events", eventId);

        // Set 30-day TTL (events older than 30 days can be reprocessed)
        redis.expire("processed_events", 86400 * 30);
    }

    /**
     * Parse JSON metadata string into Map.
     */
    private Map<String, Object> parseMetadata(String metadataJson) {
        try {
            return new ObjectMapper().readValue(metadataJson, Map.class);
        } catch (Exception e) {
            logger.warnf("Failed to parse metadata: %s", e.getMessage());
            return Map.of();
        }
    }
}
```

**Configuration (application.properties):**
```properties
# Kafka consumer for document created events
mp.messaging.incoming.repository-documents-created.connector=smallrye-kafka
mp.messaging.incoming.repository-documents-created.topic=repository.documents.created
mp.messaging.incoming.repository-documents-created.value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
mp.messaging.incoming.repository-documents-created.group.id=opensearch-indexer
mp.messaging.incoming.repository-documents-created.auto.offset.reset=earliest

# gRPC client for repository service callback
quarkus.grpc.clients.repository-service.host=repository-service
quarkus.grpc.clients.repository-service.port=9000
```

### Benefits of Callback Pattern

1. **Always fresh data**: Consumer gets latest committed state
2. **Handles rapid updates**: If event N and N+1 both update same document, final callback gets latest
3. **Schema evolution**: Event schema stays stable, gRPC response can evolve
4. **Error handling**: If callback fails, consumer can retry with exponential backoff
5. **Loose coupling**: Repository service doesn't know consumer schemas

## Event Serialization

### Protobuf Serialization

```python
# Publisher side
event = RepositoryEvent(
    event_id=generate_event_id(document_id, 'created', now_ms),
    timestamp=Timestamp(seconds=now_seconds, nanos=now_nanos),
    document_id=document_id,
    account_id=account_id,
    source=SourceContext(
        component='repository-service',
        operation='create',
        request_id=request_id,
        connector_id=connector_id
    ),
    created=RepositoryEvent.Created(
        s3_key=s3_key,
        size=size,
        content_hash=f'sha256:{sha256_hash}',
        s3_version_id=version_id
    )
)

# Serialize to bytes
event_bytes = event.SerializeToString()

# Publish to Kafka
producer.send('repository.documents.created', value=event_bytes)
```

```python
# Consumer side
message = consumer.poll()
event = RepositoryEvent()
event.ParseFromString(message.value)

# Access fields
document_id = event.document_id
s3_key = event.created.s3_key
```

### JSON Serialization (Alternative)

For non-protobuf consumers (e.g., Lambda, web hooks):

```python
# Publisher side
event_dict = {
    'event_id': generate_event_id(...),
    'timestamp': now_iso8601,
    'document_id': document_id,
    'account_id': account_id,
    'source': {
        'component': 'repository-service',
        'operation': 'create',
        'request_id': request_id
    },
    'created': {
        's3_key': s3_key,
        'size': size,
        'content_hash': content_hash
    }
}

event_json = json.dumps(event_dict)
producer.send('repository.documents.created.json', value=event_json.encode())
```

## Kafka Producer Configuration

```python
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Producer configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: v,  # Already serialized to bytes
    compression_type='gzip',
    linger_ms=10,  # Batch messages for up to 10ms
    batch_size=16384,  # 16KB batches
    acks='all',  # Wait for all replicas
    retries=3,
    max_in_flight_requests_per_connection=5,
    enable_idempotence=True  # Exactly-once delivery
)

# Create topics if not exist
admin = KafkaAdminClient(bootstrap_servers=['kafka:9092'])

topics = [
    NewTopic(name='repository.uploads.initiated', num_partitions=10, replication_factor=3),
    NewTopic(name='repository.uploads.completed', num_partitions=10, replication_factor=3),
    NewTopic(name='repository.uploads.failed', num_partitions=5, replication_factor=3),
    NewTopic(name='repository.documents.created', num_partitions=10, replication_factor=3),
    NewTopic(name='repository.documents.updated', num_partitions=10, replication_factor=3),
    NewTopic(name='repository.documents.deleted', num_partitions=5, replication_factor=3),
]

admin.create_topics(new_topics=topics, validate_only=False)
```

### Publishing Pattern

```python
def publish_upload_completed_event(upload_state, final_etag):
    """Publish upload completed event to Kafka."""
    event_id = generate_event_id(
        upload_state['node_id'],
        'upload_completed',
        now_millis()
    )

    event = RepositoryEvent(
        event_id=event_id,
        timestamp=Timestamp(seconds=now_seconds()),
        document_id=upload_state['node_id'],
        account_id=upload_state['account_id'],
        source=SourceContext(
            component='repository-service',
            operation='upload',
            request_id=upload_state['request_id'],
            connector_id=upload_state.get('connector_id')
        ),
        created=RepositoryEvent.Created(
            s3_key=upload_state['s3_key'],
            size=int(upload_state['expected_size']),
            content_hash=upload_state.get('sha256_hash', ''),
            s3_version_id=upload_state.get('s3_version_id', ''),
            s3_etag=final_etag
        )
    )

    # Async publish (fire and forget)
    future = producer.send(
        'repository.uploads.completed',
        key=upload_state['node_id'].encode(),  # Partition by document_id
        value=event.SerializeToString()
    )

    # Optional: Wait for confirmation
    try:
        record_metadata = future.get(timeout=10)
        logger.info(f'Published event {event_id} to {record_metadata.topic}:{record_metadata.partition}')
    except KafkaError as e:
        logger.error(f'Failed to publish event {event_id}: {e}')
        # Store in dead letter queue or retry queue
        store_failed_event(event, error=str(e))
```

## Error Handling

### Dead Letter Queue

If event publishing fails after retries:

```python
def store_failed_event(event, error):
    """Store failed event in Redis for later retry."""
    redis.rpush('failed_events', json.dumps({
        'event': base64.b64encode(event.SerializeToString()).decode(),
        'error': error,
        'timestamp': now_millis()
    }))

# Background job to retry failed events
def retry_failed_events():
    while True:
        event_data = redis.blpop('failed_events', timeout=60)
        if event_data:
            data = json.loads(event_data[1])
            event_bytes = base64.b64decode(data['event'])

            try:
                producer.send('repository.documents.created', value=event_bytes)
                logger.info('Successfully retried failed event')
            except KafkaError as e:
                # Re-queue if still failing
                redis.rpush('failed_events', event_data[1])
                logger.error(f'Retry failed again: {e}')
                time.sleep(60)  # Backoff
```

## Monitoring & Metrics

```python
from prometheus_client import Counter, Histogram

# Metrics
events_published = Counter(
    'repository_events_published_total',
    'Total events published',
    ['topic', 'status']
)

event_publish_latency = Histogram(
    'repository_event_publish_latency_seconds',
    'Time to publish event',
    ['topic']
)

# Track in code
with event_publish_latency.labels(topic='repository.uploads.completed').time():
    producer.send('repository.uploads.completed', value=event_bytes)

events_published.labels(topic='repository.uploads.completed', status='success').inc()
```

## Consumer Example: Self-Consumption

Repository service can consume its own events for internal state updates:

```python
class RepositoryEventConsumer:
    """Repository service consumes its own events for cache invalidation."""

    def __init__(self, cache_client):
        self.cache_client = cache_client
        self.consumer = KafkaConsumer(
            'repository.documents.updated',
            'repository.documents.deleted',
            bootstrap_servers=['kafka:9092'],
            group_id='repository-cache-invalidator',
            value_deserializer=lambda m: RepositoryEvent().ParseFromString(m)
        )

    def consume(self):
        for message in self.consumer:
            event = message.value

            # Invalidate cache for updated/deleted documents
            self.cache_client.delete(f'node:{event.document_id}')
            self.cache_client.delete(f'node:path:{event.document_id}')

            logger.info(f'Invalidated cache for document {event.document_id}')
```

## Schema Registry Integration with Apicurio

**Our established pattern:**
- **Key**: UUID (document_id) as string
- **Value**: Protobuf message (RepositoryEvent)
- **Schema Management**: Apicurio Registry

### Apicurio Setup

```yaml
# docker-compose.yml
version: '3.8'
services:
  apicurio:
    image: apicurio/apicurio-registry-mem:latest
    ports:
      - "8080:8080"
    environment:
      REGISTRY_AUTH_ANONYMOUS_READ_ACCESS_ENABLED: "true"
      REGISTRY_AUTH_ANONYMOUS_WRITE_ACCESS_ENABLED: "true"  # Dev only!
```

### Producer Configuration with Apicurio

```python
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

# Configure Apicurio schema registry
schema_registry_conf = {'url': 'http://apicurio:8080/apis/registry/v2'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Protobuf serializer with Apicurio
protobuf_serializer = ProtobufSerializer(
    RepositoryEvent,  # Your protobuf message class
    schema_registry_client,
    conf={'use.deprecated.format': False}
)

# String serializer for UUID keys
string_serializer = StringSerializer('utf_8')

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'compression.type': 'gzip',
    'linger.ms': 10,
    'batch.size': 16384,
    'acks': 'all',
    'retries': 3,
    'max.in.flight.requests.per.connection': 5,
    'enable.idempotence': True
}

producer = Producer(producer_conf)

def publish_event(event, topic):
    """
    Publish event to Kafka with UUID key and Protobuf value.

    Key: document_id (UUID string)
    Value: RepositoryEvent (Protobuf, managed by Apicurio)
    """
    try:
        producer.produce(
            topic=topic,
            key=string_serializer(event.document_id, None),  # UUID as string
            value=protobuf_serializer(
                event,
                SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=delivery_report
        )
        producer.poll(0)  # Trigger callbacks
    except Exception as e:
        logger.error(f'Failed to produce event: {e}')
        raise

def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
```

### Consumer Configuration with Apicurio

```python
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

# String deserializer for UUID keys
string_deserializer = StringDeserializer('utf_8')

# Protobuf deserializer with Apicurio
protobuf_deserializer = ProtobufDeserializer(
    RepositoryEvent,
    schema_registry_client,
    conf={'use.deprecated.format': False}
)

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'repository-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Manual commit for exactly-once
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['repository.documents.created', 'repository.documents.updated'])

def consume_events():
    """Consume events with UUID keys and Protobuf values."""
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f'Reached end of partition {msg.partition()}')
                else:
                    logger.error(f'Consumer error: {msg.error()}')
                continue

            # Deserialize key (UUID)
            document_id = string_deserializer(msg.key(), None)

            # Deserialize value (Protobuf)
            event = protobuf_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )

            # Process event
            process_event(document_id, event)

            # Commit offset after successful processing
            consumer.commit()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
```

### Apicurio Schema Registration

Apicurio automatically registers schemas on first use, but you can pre-register:

```python
from confluent_kafka.schema_registry import Schema

# Register RepositoryEvent schema
schema_str = """
syntax = "proto3";

package ai.pipestream.repository.filesystem;

import "google/protobuf/timestamp.proto";

message RepositoryEvent {
  string event_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  string document_id = 3;
  string account_id = 4;
  // ... rest of schema
}
"""

schema = Schema(schema_str, schema_type="PROTOBUF")

# Register schema with Apicurio
schema_registry_client.register_schema(
    subject_name='repository.documents.created-value',
    schema=schema
)
```

### Schema Evolution with Apicurio

Apicurio supports forward and backward compatibility:

```python
# Configure compatibility mode
schema_registry_client.set_compatibility(
    subject_name='repository.documents.created-value',
    level='BACKWARD'  # New schema can read old data
)

# Allowed changes (backward compatible):
# - Add optional fields
# - Add oneof fields
# - Add enum values

# Disallowed changes (breaks backward compatibility):
# - Remove required fields
# - Change field numbers
# - Change field types
```

### Debugging Apicurio

```bash
# List all schemas
curl http://apicurio:8080/apis/registry/v2/groups/default/artifacts

# Get specific schema
curl http://apicurio:8080/apis/registry/v2/groups/default/artifacts/repository.documents.created-value

# Check compatibility
curl -X POST http://apicurio:8080/apis/registry/v2/groups/default/artifacts/repository.documents.created-value/versions \
  -H "Content-Type: application/json" \
  -d @new-schema.json
```

### Apicurio in Quarkus

If using Quarkus, configure Apicurio in `application.properties`:

```properties
# Kafka connection
kafka.bootstrap.servers=kafka:9092

# Apicurio registry
mp.messaging.connector.smallrye-kafka.apicurio.registry.url=http://apicurio:8080/apis/registry/v2
mp.messaging.connector.smallrye-kafka.apicurio.registry.auto-register=true
mp.messaging.connector.smallrye-kafka.apicurio.registry.use-id=contentId

# Outgoing channel (producer)
mp.messaging.outgoing.repository-events.connector=smallrye-kafka
mp.messaging.outgoing.repository-events.topic=repository.documents.created
mp.messaging.outgoing.repository-events.value.serializer=io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer
mp.messaging.outgoing.repository-events.key.serializer=org.apache.kafka.common.serialization.StringSerializer

# Incoming channel (consumer)
mp.messaging.incoming.repository-events.connector=smallrye-kafka
mp.messaging.incoming.repository-events.topic=repository.documents.created
mp.messaging.incoming.repository-events.value.deserializer=io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer
mp.messaging.incoming.repository-events.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.repository-events.group.id=repository-consumer
```

### Common Apicurio Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| `Schema not found` | Producer hasn't registered schema yet | Enable auto-register or pre-register schema |
| `Incompatible schema` | Breaking change in protobuf | Use backward-compatible changes only |
| `Connection refused` | Apicurio not running | Check docker-compose, verify port 8080 |
| `Deserialization error` | Schema version mismatch | Ensure consumer fetches latest schema from Apicurio |

### Best Practices

1. **Always use UUID for keys**: Ensures partitioning by document_id
2. **Auto-register in dev, pre-register in prod**: Avoid schema registration failures in production
3. **Set compatibility mode**: Enforce backward/forward compatibility
4. **Monitor schema versions**: Track schema evolution over time
5. **Test schema changes**: Validate compatibility before deploying

## Next: Architecture Comparison

See [05-comparison.md](05-comparison.md) for a detailed comparison with the legacy architecture.
