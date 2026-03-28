# Repository Service: Redis Hot Cache Layer

## Overview

Add an optional Redis cache layer to the repository service's document storage. Three storage modes selectable at runtime via configuration, giving operators control over the speed-vs-durability tradeoff per deployment.

This system is NOT the system of record. Source data lives in connectors (JDBC databases, S3 buckets) and can be re-crawled. The repository is a processing cache with eventual S3 persistence.

## Storage Modes

### Mode 1: `s3-only` (current default)

```
store() → S3 putObject (wait for ACK) → DB upsert → emit events → return
get()   → DB lookup → S3 getObject → parse protobuf → return
```

- Synchronous S3 write before ACK
- Durable immediately
- ~30-50ms per document (S3 round-trip)
- Use when: compliance requirements, critical one-shot uploads, small batches

### Mode 2: `redis-buffered` (new)

```
store() → Redis SET (sub-ms) → DB upsert (status=PENDING_S3) → emit events → return
          └─ background flusher: Redis → S3 putObject → DB status=AVAILABLE
get()   → DB lookup → Redis GET (hit?) → yes: return | no: S3 getObject → return
```

- Redis write is the ACK — sub-millisecond
- S3 write happens asynchronously in background
- Documents available for pipeline processing immediately from Redis
- ~80% reduction in ACK time
- Use when: batch ingestion, reprocessing-heavy workloads, re-crawlable sources

### Mode 3: `grpc-passthrough` (existing behavior for inter-step transport)

The engine already supports this via `transport_type=GRPC`. Documents pass between pipeline steps in memory via gRPC calls. The repository is only hit at intake (first store) and final output. No changes needed — this mode is about the engine's transport, not the repository's storage.

## Redis Cache Design

### Key Structure

```
pipedoc:{nodeId} → serialized PipeDoc protobuf bytes
```

NodeId is the deterministic UUID generated from `(docId, graphAddressId, accountId)`. Same key the DB uses, same key S3 uses in the object path.

### TTL Strategy

- Default: **8 hours** (configurable via `repo.redis.ttl-hours`)
- Adjustable at runtime without restart (Quarkus config hot-reload or Redis CONFIG SET)
- TTL is a safety net — the background flusher should drain documents to S3 well before expiry
- If a document is still being actively read (reprocessing), each `get()` hit can optionally refresh the TTL

### Redis Persistence (Production)

- **Not required** for correctness — data loss on Redis failure means re-crawl, not permanent loss
- **Recommended** for large batch jobs where re-crawl cost is high
- Enable Redis AOF (`appendonly yes`) in production for crash recovery
- Dev/test: no persistence needed, Quarkus DevServices auto-provisions Redis

## Background S3 Flusher

A `@Scheduled` Quarkus bean that continuously drains Redis to S3:

```
1. Query DB for PipeDocRecords where status = PENDING_S3
2. For each record:
   a. Redis GET pipedoc:{nodeId}
   b. S3 putObject
   c. DB update status = AVAILABLE
   d. (optionally) Redis DEL — or let TTL handle cleanup
3. On S3 failure: increment retry count, log error, skip to next
4. On Redis miss (TTL expired before flush): mark status = LOST, emit error event
```

### Flush Interval

- Default: every **5 seconds** (`repo.redis.flush-interval-seconds=5`)
- Batch size: up to 50 documents per flush cycle
- Configurable for throughput tuning

### Error Reporting via Streaming RPC

When the flusher encounters a lost document (Redis TTL expired before S3 flush), it emits an error event. Connectors can subscribe to a streaming RPC to monitor storage health:

```protobuf
// Addition to repository service proto
service RepositoryStorageService {
  // Stream storage health events — flush failures, lost documents, S3 errors.
  // Connectors and admin tools subscribe to monitor ingestion reliability.
  rpc WatchStorageHealth(WatchStorageHealthRequest) returns (stream StorageHealthEvent);
}

message WatchStorageHealthRequest {
  // Optional filter by account_id or datasource_id
  optional string account_id = 1;
  optional string datasource_id = 2;
}

message StorageHealthEvent {
  string event_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  StorageHealthEventType event_type = 3;
  string document_id = 4;
  string account_id = 5;
  string datasource_id = 6;
  string message = 7;

  // For FLUSH_FAILED: the error details
  optional string error_message = 8;
  // For DOCUMENT_LOST: the node_id that was lost
  optional string node_id = 9;
  // Retry count so far
  int32 retry_count = 10;
}

enum StorageHealthEventType {
  STORAGE_HEALTH_EVENT_TYPE_UNSPECIFIED = 0;
  // S3 flush succeeded (informational, high-volume — filter in production)
  STORAGE_HEALTH_EVENT_TYPE_FLUSHED = 1;
  // S3 flush failed, will retry
  STORAGE_HEALTH_EVENT_TYPE_FLUSH_FAILED = 2;
  // Document lost — Redis TTL expired before successful S3 flush
  STORAGE_HEALTH_EVENT_TYPE_DOCUMENT_LOST = 3;
  // Redis connection error
  STORAGE_HEALTH_EVENT_TYPE_REDIS_ERROR = 4;
}
```

This lets connectors know when documents need re-crawling. The testing sidecar or admin UI can subscribe to display real-time storage health.

## Configuration

```properties
# Storage mode: s3-only (default) | redis-buffered
repo.storage.mode=s3-only

# Redis cache settings (only used when mode=redis-buffered)
repo.redis.ttl-hours=8
repo.redis.flush-interval-seconds=5
repo.redis.flush-batch-size=50
repo.redis.refresh-ttl-on-read=true

# Quarkus Redis connection (auto-configured by DevServices in dev)
quarkus.redis.hosts=redis://localhost:6379
%dev.quarkus.redis.devservices.enabled=true
```

All settings are hot-reloadable. Switching from `s3-only` to `redis-buffered` at runtime is safe — new stores go to Redis, existing S3 documents are still readable. Switching back to `s3-only` is also safe — the flusher continues draining any remaining Redis documents.

## Code Changes

### Files Modified

| File | Change |
|------|--------|
| `build.gradle` | Add `quarkus-redis-client` dependency |
| `DocumentStorageService.java` | Branch `store()` and `get()` on storage mode config |
| `application.properties` | Add `repo.storage.mode` + Redis config properties |

### Files Created

| File | Purpose |
|------|---------|
| `RedisDocumentCache.java` | `get(nodeId)`, `put(nodeId, bytes, ttl)`, `delete(nodeId)` — thin wrapper around Quarkus Redis reactive client |
| `BackgroundS3Flusher.java` | `@Scheduled` bean, drains PENDING_S3 records from DB, reads from Redis, writes to S3, updates status |
| `StorageMode.java` | Enum: `S3_ONLY`, `REDIS_BUFFERED` |
| `StorageHealthEmitter.java` | Emits `StorageHealthEvent` for the streaming RPC |

### Files NOT Modified

Everything outside the repository service. No changes to:
- Connectors (JDBC, S3)
- Intake service
- Engine / sidecar
- Modules (parser, chunker, embedder, etc.)
- OpenSearch manager
- Protos (except adding the optional `WatchStorageHealth` RPC)

The `store()` and `get()` method signatures don't change. Callers are unaware of the storage mode.

## Performance Impact

| Scenario | S3-only | Redis-buffered | Improvement |
|----------|---------|----------------|-------------|
| Single doc store | ~40ms | ~1ms | 40x faster ACK |
| 1000-doc JDBC crawl (store phase) | ~40s | ~1s | 40x faster |
| Pipeline reprocessing read (within TTL) | ~30ms (S3) | ~0.5ms (Redis) | 60x faster |
| Pipeline reprocessing read (after TTL) | ~30ms (S3) | ~30ms (S3 fallback) | No change |
| 10K-doc batch ingestion | ~400s | ~10s + background flush | ~40x faster ACK |

The background flusher adds S3 write latency but it's decoupled from the request path. The caller doesn't wait for it.

## Risk Analysis

| Risk | Severity | Mitigation |
|------|----------|------------|
| Redis crash before S3 flush | Medium | DB tracks `PENDING_S3` status. Re-crawl source data. Enable AOF in production for persistence. |
| Redis memory exhaustion | Low | TTL ensures cleanup. 10K docs at 50KB avg = ~500MB. Monitor with Redis INFO. |
| Flusher falls behind | Low | Tune flush interval and batch size. Add metrics (pending count, flush rate). |
| Mode switch during active processing | None | Safe — new stores use new mode, existing data accessible from both paths. |

## Not the System of Record

This design explicitly acknowledges that the repository service is a **processing cache**, not permanent storage:

- Source data lives in connectors (databases, S3 buckets, file systems)
- Documents can always be re-crawled from the source
- The `DOCUMENT_LOST` event tells connectors "re-send this document"
- S3 is the durable tier, but even S3 loss is recoverable via re-crawl
- Redis is the speed tier — ephemeral by design

## Streaming RPC: Yes or No?

**Recommendation: Yes, add `WatchStorageHealth` to the repository proto.**

Reasons:
- Connectors already have a gRPC connection to the repository service
- The flusher naturally produces events that should be observable
- Admin UI / testing sidecar can display real-time storage health
- The streaming pattern already exists in the platform (engine's `WatchHealth`, shell service's `WatchHealth`)
- It's a small proto addition with high operational value

The alternative (Kafka topic for flush events) works too but adds a Kafka dependency for what's really a point-to-point monitoring concern. A gRPC stream is simpler and lower latency.

## Implementation Order

1. Add `quarkus-redis-client` + config properties
2. `RedisDocumentCache` — thin GET/SET/DEL wrapper
3. `DocumentStorageService` — branch store/get on mode
4. `BackgroundS3Flusher` — scheduled drain worker
5. `WatchStorageHealth` proto + `StorageHealthEmitter`
6. Test: run S3 crawl E2E with `redis-buffered` mode, compare times
7. Test: kill Redis mid-crawl, verify flusher reports DOCUMENT_LOST, re-crawl recovers
