# Phase 1 Implementation - Intake Fast ACK Pattern

This document describes the implementation of Phase 1 as specified in [phase-1-intake-fast-ack.md](phase-1-intake-fast-ack.md).

## Overview

Phase 1 implements the intake-only fast ACK path with in-memory state management. No persistence to Redis, Kafka, S3, or MySQL occurs in this phase. The purpose is to validate the API surface area, handler overhead, and establish metrics/tracing for Phase 2 throughput testing.

## Implementation Structure

```
src/main/java/ai/pipestream/repository/
├── config/
│   ├── IntakeConfiguration.java        # Configuration for intake service
│   └── FeatureFlags.java               # Feature flag for Phase 1
└── intake/
    ├── InMemoryUploadState.java        # Per-upload ephemeral state
    ├── InMemoryUploadStateStore.java   # Bounded cache with eviction
    ├── IntakeMetrics.java              # Micrometer metrics
    └── NodeUploadGrpcService.java      # gRPC service implementation

src/test/java/ai/pipestream/repository/intake/
├── InMemoryUploadStateTest.java        # Unit tests for state idempotency
├── InMemoryUploadStateStoreTest.java   # Unit tests for state store
└── NodeUploadGrpcServiceTest.java      # Integration tests for gRPC service
```

## Configuration

Add the following to `application.properties`:

```properties
# Phase 1 feature gate
repo.features.phase1.enabled=true

# In-memory state caps
repo.intake.memory.max-uploads=50000
repo.intake.memory.idle-ttl=PT30M

# Progress stream interval
repo.intake.progress.interval=PT0.5S

# Checksum validation (off for Phase 1)
repo.intake.checksum.validate=false

# gRPC message limits (for large chunks in Phase 2 tests)
quarkus.grpc.server.max-inbound-message-size=268435456  # 256MB
```

## gRPC Service Implementation

The `NodeUploadGrpcService` implements the `NodeUploadServiceGrpc.NodeUploadServiceImplBase` with the following RPCs:

### InitiateUpload

- Generates `node_id` (UUID or client-provided) and `upload_id` (UUID)
- Creates in-memory state entry
- Returns immediately with metadata
- Records `upload_initiated_total` metric

### UploadChunk

- Validates `upload_id` exists in memory, `chunk_number >= 0`, and payload is non-empty
- Implements idempotency: if `(upload_id, chunk_number)` already received, returns OK without reprocessing
- Marks chunk as received, increments bytes counter
- Updates `last_activity_ts`
- Records `upload_chunk_received_total` and `upload_chunk_payload_bytes` metrics

### GetUploadStatus

- Returns synthetic status from in-memory counters
- Includes `bytes_uploaded`, `updated_at_epoch_ms`
- State is always `UPLOAD_STATE_UPLOADING` in Phase 1

### StreamUploadProgress

- Server-streaming RPC for real-time progress updates
- Sends progress at configurable intervals (default 500ms)
- Includes current `bytes_uploaded` and timestamp

### CancelUpload

- Removes upload state from memory
- Returns success/failure status

## Metrics Exposed

The following metrics are available at `/q/metrics`:

### Counters
- `upload_initiated_total` - Total uploads initiated
- `upload_chunk_received_total` - Total chunks received
- `upload_chunk_idempotent_hit_total` - Duplicate chunks (idempotent hits)
- `upload_stream_clients_total` - Progress stream clients

### Timers (with p50/p95/p99 percentiles)
- `rpc_initiate_upload_latency_ms`
- `rpc_upload_chunk_ack_latency_ms`
- `rpc_get_status_latency_ms`

### Distribution Summary
- `upload_chunk_payload_bytes` - Chunk size distribution

### Gauges
- `in_memory_uploads_active` - Current active uploads
- `in_memory_uploads_evicted_total` - Total evicted uploads

## In-Memory State Management

Uses Guava's `Cache` with:
- Bounded size (`max-uploads`)
- TTL-based eviction (`idle-ttl`)
- Automatic metrics registration
- Thread-safe operations via `ConcurrentHashMap.KeySetView` for chunk tracking
- NodeId to UploadId index for efficient lookups

## Running Locally

```bash
# Start in dev mode
./gradlew quarkusDev

# Enable Phase 1 (already enabled by default)
# Set repo.features.phase1.enabled=true
```

## Testing

### Unit Tests

```bash
./gradlew test --tests "ai.pipestream.repository.intake.*"
```

Tests cover:
- State creation and retrieval
- Chunk idempotency (duplicate chunks don't increase byte count)
- Out-of-order chunk handling
- Concurrent chunk receipt
- State eviction and cleanup

### Integration Tests

The `NodeUploadGrpcServiceTest` provides full gRPC integration tests:
- Complete upload flow
- Idempotency validation
- Error handling (invalid chunk numbers, empty data)
- Status tracking
- Upload cancellation

### Smoke Test (Manual)

1. Start the service: `./gradlew quarkusDev`
2. Use a gRPC client to:
   - Call `InitiateUpload` and capture `{upload_id, node_id}`
   - Fire N parallel `UploadChunk` calls with random `chunk_number`
   - Call `GetUploadStatus`
   - Attach to `StreamUploadProgress`
3. Verify metrics at `/q/metrics`

## Dependencies Added

- `com.google.guava:guava:33.3.1-jre` - In-memory cache with eviction
- `io.quarkus:quarkus-micrometer` - Metrics
- `io.quarkus:quarkus-micrometer-registry-prometheus` - Prometheus exporter
- `io.quarkus:quarkus-opentelemetry` - Distributed tracing

## Tracing

OpenTelemetry integration provides:
- gRPC server spans
- Attributes: `upload_id`, `node_id`, `chunk_number`
- Trace ID in JSON logs via MDC

## Known Limitations (Phase 1)

1. **No persistence** - All state is ephemeral and lost on restart
2. **Memory constraints** - Bounded by `max-uploads` setting
3. **No completion detection** - Status is always `UPLOADING`
4. **No checksum validation** - Disabled by default
5. **Progress stream doesn't terminate** - Runs until client cancels

## Next Steps

See [phase-2-throughput-validation.md](phase-2-throughput-validation.md) for throughput testing phase.

## Files Modified

- `build.gradle` - Added Guava, Micrometer, and OpenTelemetry dependencies
- `src/main/resources/application.properties` - Added Phase 1 configuration
- `src/test/resources/import.sql` - Created empty import file for tests

## Files Created

### Production Code
- `src/main/java/ai/pipestream/repository/config/IntakeConfiguration.java`
- `src/main/java/ai/pipestream/repository/config/FeatureFlags.java`
- `src/main/java/ai/pipestream/repository/intake/InMemoryUploadState.java`
- `src/main/java/ai/pipestream/repository/intake/InMemoryUploadStateStore.java`
- `src/main/java/ai/pipestream/repository/intake/IntakeMetrics.java`
- `src/main/java/ai/pipestream/repository/intake/NodeUploadGrpcService.java`

### Test Code
- `src/test/java/ai/pipestream/repository/intake/InMemoryUploadStateTest.java`
- `src/test/java/ai/pipestream/repository/intake/InMemoryUploadStateStoreTest.java`
- `src/test/java/ai/pipestream/repository/intake/NodeUploadGrpcServiceTest.java`
