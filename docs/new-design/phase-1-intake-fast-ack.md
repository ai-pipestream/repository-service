# Phase 1 Runbook — Intake-only Fast ACK Path (no persistence)

This document is the step-by-step implementation and verification guide for Phase 1. It turns on only the intake surface and immediate acknowledgments. No Redis, Kafka, S3, or MySQL writes occur in this phase. The purpose is to validate the API surface area, handler overhead, and plumbing for metrics/tracing so we can run high-speed tests in Phase 2.

Status: Ready to implement

## Objectives

- Implement `InitiateUpload`, `UploadChunk`, `GetUploadStatus`, and `StreamUploadProgress` handlers with minimal, in-memory behavior.
- Accept chunks and respond immediately (fast ACK) without persistence.
- Allow out-of-order chunks and accept arbitrary chunk sizes (consistent with user decisions).
- Establish metrics, tracing, and logging with correlation IDs to power Phase 2 throughput tests.

## Non-goals

- No Redis, Kafka, S3, or MySQL integration.
- No durability guarantees or long-term status tracking.

## Interfaces (RPCs)

Protos are defined in platform libraries; we do not change proto in this phase. Server implements:

- InitiateUpload(request)
  - Input: minimal metadata (KISS). Suggested optional fields accepted but not required: `filename`, `mime_type`, `drive_id`, `tags`. No `expected_size` enforcement.
  - Output: `{ node_id (UUID), upload_id (UUID), created_at (ts) }`.

- UploadChunk(request)
  - Input: `{ upload_id, node_id, chunk_number (int), bytes (payload), checksum? (optional), force? (optional boolean) }`.
  - Behavior: Accept any size. Out-of-order allowed. If `(upload_id, chunk_number)` seen before with same size/hash (if provided), return OK without reprocessing unless `force==true`.
  - Output: `{ ack: OK, received_bytes, chunk_number }` (extend as per proto).

- GetUploadStatus(request)
  - Input: `{ upload_id }`.
  - Output: Synthetic status derived from in-memory counters: `RECEIVED_CHUNKS`, `LAST_ACTIVITY_TS`. Mark `COMPLETION` as `UNKNOWN` in this phase.

- StreamUploadProgress(request) [server-streaming]
  - Input: `{ upload_id }`.
  - Output: Periodic synthetic progress events based on in-memory counters; ends only if client cancels.

## Handler behavior (in-memory)

- Store per-upload ephemeral state in a bounded in-memory map (Caffeine or simple ConcurrentHashMap) with eviction to prevent leaks.
  - Key: `upload_id`
  - Value: `{ node_id, created_at, received_chunks: ConcurrentBitSet or IntSet, bytes_received_total, last_activity_ts }`
  - Eviction: `max_entries` and `idle_ttl` (defaults: 50k uploads, 30 minutes idle). Configurable.

- InitiateUpload
  - Generate `node_id` and `upload_id` (UUIDv4 by default).
  - Insert empty upload state in memory.
  - Log and emit `upload_initiated_total` metric.
  - Return immediately.

- UploadChunk
  - Validate: `upload_id` present in memory; `chunk_number >= 0`; payload non-null.
  - Idempotency: if `chunk_number` already marked received and `force != true`, return OK without work. If `force==true`, treat as fresh receipt.
  - Mark `chunk_number` as received and increment `bytes_received_total`.
  - Update `last_activity_ts`.
  - Emit metrics (see below) and return immediately.

- GetUploadStatus
  - If unknown `upload_id`, return `NOT_FOUND` or `{status: UNKNOWN}` based on proto semantics.
  - Return synthetic counters.

- StreamUploadProgress
  - Periodically send current counters (e.g., every 500 ms configurable) until client cancels.

## Validation rules

- Chunk size: no minimum enforced (S3 5 MB constraint applies later in Phase 5). We simply accept any size in Phase 1.
- Ordering: out-of-order allowed.
- Checksums: not required. If provided, record and validate only if `enable_checksum_validation=true` (default false in Phase 1).
- Concurrency: no hard per-upload limit in Phase 1; rely on server global concurrency settings and connection limits. We will measure in Phase 2 to inform future caps.

## Metrics, tracing, and logging

Expose the following metrics via Micrometer/MP Metrics and Prometheus:

- Counters
  - `upload_initiated_total`
  - `upload_chunk_received_total`
  - `upload_chunk_idempotent_hit_total` (duplicate chunk without force)
  - `upload_stream_clients_total`

- Timers/Histograms
  - `rpc_initiate_upload_latency_ms` (p50/p95/p99)
  - `rpc_upload_chunk_ack_latency_ms` (p50/p95/p99)
  - `rpc_get_status_latency_ms`
  - `upload_chunk_payload_bytes` (DistributionSummary)

- Gauges
  - `in_memory_uploads_active`
  - `in_memory_uploads_evicted_total`

Tracing: enable OpenTelemetry with gRPC server spans. Tag/attributes:
- `upload_id`, `node_id`, `chunk_number`, `client_id` (if available)

Logging: JSON logs containing `upload_id`, `node_id`, `chunk_number`, and request `trace_id`.

## Configuration (example keys)

application.properties (dev defaults):

```
# Phase 1 feature gate
repo.features.phase1.enabled=true

# In-memory state caps
repo.intake.memory.maxUploads=50000
repo.intake.memory.idleTtl=30m

# Progress stream
repo.intake.progress.interval=500ms

# Checksum validation (off for Phase 1)
repo.intake.checksum.validate=false

# gRPC message limits (raise to allow large chunks in Phase 2 tests)
quarkus.grpc.server.max-inbound-message-size=268435456 # 256MB

# Micrometer/OTel exports (Prometheus + OTLP if available)
quarkus.micrometer.binder.http-server.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://localhost:4317
```

All keys should be namespaced under `repo.intake.*` and documented in README.

## Local dev & smoke tests

1) Start the app: `./gradlew quarkusDev`
2) Use a small client to:
   - Call `InitiateUpload` and capture `{upload_id, node_id}`
   - Fire N parallel `UploadChunk` calls with random `chunk_number`
   - Call `GetUploadStatus`
   - Optionally attach to `StreamUploadProgress`
3) Verify metrics endpoint exposes counters and timers.

Deliverables for Phase 1 completion:
- Handlers implemented and guarded by `repo.features.phase1.enabled`.
- Unit tests for idempotency and validation.
- Minimal smoke test tool or instructions to use connector-intake-service as the client.
- README snippet with configuration and usage.

## Acceptance criteria (DOD)

- Build passes; health check UP.
- RPC handlers return correct responses and handle out-of-order and duplicate chunks per rules.
- Metrics/tracing/logging available with correlation IDs.
- Able to sustain a short (e.g., 1–2 minutes) high-QPS smoke run without OOM or handler errors.

## Risks & mitigations

- Memory growth: set conservative `maxUploads` and `idleTtl` to prevent leaks.
- Large messages: ensure `max-inbound-message-size` is adequately set; document client matching limits.
- Duplicate chunks without checksum: idempotency relies on `(upload_id, chunk_number)`. In Phase 1 this is acceptable; checksum-based verification can be added later.

## Next phase handoff

Once the handlers and metrics are stable, proceed to Phase 2 to execute structured throughput tests and tune the network/runtime stack before introducing persistence overhead.
