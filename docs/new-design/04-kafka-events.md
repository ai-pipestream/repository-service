# Kafka Events (Lean Events + Callback Pattern)

This document captures the **Kafka philosophy** for repository-service.

- Events are **lean** (state transitions + identifiers)
- The repository DB + repo-service API are the **authoritative source**
- Consumers fetch details via **callback** (gRPC/HTTP) when needed
- No dedicated external cache/queue is used for dedupe, retry queues, or coordination

## Event Design Principles

### Lean events

- Keep Kafka messages small and stable.
- Include enough IDs for consumers to call back:
  - `account_id`, `connector_id` (if applicable)
  - `doc_id` (stable ID)
  - `checksum` (version discriminator)
  - optional `s3_bucket/s3_key/s3_version_id` when relevant

### Deterministic event IDs

Use a deterministic `event_id` (e.g., hash of `(doc_id, checksum, event_type, logical_time)`) so consumers can be idempotent.

### Source context

Include a `SourceContext` (component, request_id, connector_id, etc.) for auditability and trace correlation.

## Topics

We prefer **one logical event stream** for repository changes, keyed by `doc_id`:

- `repository.events`

(If you later split by entity, keep the same principles.)

## Callback Pattern

**Problem:** Kafka events are lean, but consumers often need more data.

**Solution:** Consumers call repo-service APIs to fetch authoritative state.

Example:

- consumer gets `RepositoryEvent{doc_id, checksum, ...}`
- consumer calls `PipeDocService.GetPipeDoc` (or an API endpoint) to fetch:
  - metadata only
  - or hydrated payload if needed (policy-driven)

This avoids:

- duplicating DB state in consumers
- large Kafka payloads
- race conditions from “event contains the full doc” patterns

## Confirmed-step progress

Instead of “live byte progress” everywhere, we treat progress as **confirmed steps** via events (works for all upload modes):

- `RECEIVED`
- `STORED_BLOB` (optional)
- `STORED_PIPEDOC`
- `EMITTED_EVENT`

See `../../DESIGN.md` for the lifecycle state machine.

## Apicurio + Protobuf (Quarkus)

Repository-service uses Protobuf schemas in Apicurio and Protobuf serdes for Kafka.

Typical Quarkus config shape (adjust to your channel names):

```properties
kafka.bootstrap.servers=kafka:9092

mp.messaging.connector.smallrye-kafka.apicurio.registry.url=http://apicurio:8080/apis/registry/v2
mp.messaging.connector.smallrye-kafka.apicurio.registry.auto-register=true

mp.messaging.outgoing.repository-events.connector=smallrye-kafka
mp.messaging.outgoing.repository-events.topic=repository.events
mp.messaging.outgoing.repository-events.value.serializer=io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer
mp.messaging.outgoing.repository-events.key.serializer=org.apache.kafka.common.serialization.StringSerializer

mp.messaging.incoming.repository-events.connector=smallrye-kafka
mp.messaging.incoming.repository-events.topic=repository.events
mp.messaging.incoming.repository-events.value.deserializer=io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer
mp.messaging.incoming.repository-events.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

## Idempotency guidance for consumers

Consumers can dedupe with one of:

- a small **DB table** keyed by `event_id`
- Kafka transactional/idempotent processing where applicable
- storage-level idempotency (e.g., OpenSearch upsert by `doc_id` + `checksum`)

## Related

- `../../DESIGN.md` (authoritative)
- `03-s3-multipart.md` (multipart reference; future mode)
