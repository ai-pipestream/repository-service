Phased Delivery Plan (incremental, testable milestones)

This document breaks the new design into small, verifiable phases. We will start with intake-only (no persistence), prove throughput, then add persistence layers one at a time until the full lifecycle to S3 is complete.

Conventions:
- DOR = Definition of Ready, DOD = Definition of Done
- SLA/SLO targets will be finalized from open-questions.md responses and may be adjusted.

Phase 0: Compile and health (baseline)
- Goal: Service compiles and basic health checks work in dev.
- Scope: Fix compile errors, health endpoints, minimal config.
- DOD: ./gradlew build succeeds; readiness reports UP in dev.

Phase 1: Intake-only fast ACK path (no persistence)
- Goal: Accept InitiateUpload and UploadChunk RPCs and respond immediately without persistence, to validate API surface and handler overhead.
- Scope:
  - Implement stubs for InitiateUpload, UploadChunk, GetUploadStatus returning synthetic values and in-memory counters only.
  - Enforce basic request validation (ids, chunk_number, sizes).
  - Emit metrics for QPS, p50/p95 latency, payload sizes.
- Non-Goals: Redis/Kafka/S3 integration.
- Perf Target (provisional): p95 UploadChunk ACK < 10-20 ms with 16 MB chunk on dev hardware; sustained ingest >= X MB/s per pod (TBD).
- Test Plan: Use intake-service/load tool to drive N parallel clients; export Prometheus metrics; verify no GC/memory regressions.
- DOD:
  - RPC handlers implemented, unit and smoke tests in CI.
  - Metrics show stable low latency under target load for >=10 minutes.

Phase 2: Throughput validation between intake -> repository-service
- Goal: Prove we can push chunks from intake-service to repository-service very fast and at scale before adding any persistence costs.
- Scope:
  - Provide a simple benchmark profile with tunable concurrency, chunk sizes, and payload source.
  - Document tuning knobs (thread pools, netty, gRPC message limits, JVM flags).
- Success Criteria (gate): Achieve agreed sustained throughput and error rate from open-questions.md answers.
- DOD:
  - Report with throughput, latencies, CPU, RAM, and network utilization.
  - Bottlenecks identified and mitigations listed.

Phase 3: Redis coordination layer (state + queue) only
- Goal: Introduce Redis for upload state and chunk queuing, still without S3/Kafka.
- Scope:
  - Keys: upload:{upload_id}:state, upload:{upload_id}:chunks:{n}, upload:{upload_id}:etags, upload:{upload_id}:metadata as per overview.
  - TTL policies and memory limits.
  - Idempotent UploadChunk writes; dedupe by (upload_id, chunk_number).
  - Background workers dequeue chunks and simulate processing (no S3 yet) to measure overhead of Redis.
- Perf Target: Maintain a high percentage (>=80-90%) of Phase 2 throughput.
- DOD:
  - Redis integration behind feature flag.
  - Monitoring: Redis queue depth, op latency, memory.
  - Back-pressure behavior documented (429 vs. buffering) and implemented.

Phase 4: Kafka events (lightweight)
- Goal: Publish minimal events for state changes to validate schemas and partitions.
- Scope:
  - Emit UploadInitiated, UploadCompleted, UploadFailed; ChunkUploaded optional behind flag.
  - Topic naming, partitions, keys (likely upload_id), Apicurio schemas.
- DOD:
  - Events visible in Kafka; consumer stub validates schema.
  - End-to-end ingest still meets acceptable throughput (>=70-85% of Phase 2).

Phase 5: S3/MinIO multipart upload processing
- Goal: Replace simulated workers with real S3 multipart uploads.
- Scope:
  - InitiateUpload initiates S3 multipart and writes upload state.
  - Workers upload parts and collect ETags into Redis sorted set.
  - When complete, issue S3 CompleteMultipartUpload with ordered ETags.
  - Edge cases: small objects threshold for single PUT; retries with exponential backoff.
- Data Integrity:
  - Optional per-chunk checksum verification; required overall SHA256 at completion (TBD).
- DOD:
  - Objects appear in MinIO/S3; ETags recorded; completion events published.
  - Abort on failure path tested; orphan detection/cleanup job documented.

Phase 6: Persistence of metadata (MySQL) and lifecycle
- Goal: Durable node/drive metadata and final lifecycle closing.
- Scope:
  - Persist node record (drive_id, node_id, size, mime, hash, s3_key, timestamps).
  - Transactional consistency: write-after S3 completion; retry policy.
  - Idempotency on replays (no duplicate rows).
- DOD:
  - Schema migrations applied; repository DAO tests; recovery verified.

Phase 7: Full lifecycle hardening, reliability, and ops
- Goal: Production-readiness.
- Scope:
  - Crash recovery: resume incomplete uploads from Redis state.
  - Compaction/cleanup: TTL enforcement, abort stale S3 uploads, delete expired Redis chunk keys.
  - Security: mTLS/JWT, authZ checks per tenant/drive.
  - Observability: tracing spans (gRPC, Redis, S3, Kafka), dashboards, alerts.
  - Load/cost controls: rate limits, quotas, per-tenant isolation (as required).
- DOD:
  - Playbooks for on-call; SLOs documented; runbooks for common incidents.

Cross-cutting deliverables
- Benchmarks: repeatable benchmark harness and scripts.
- Feature flags: allow toggling Redis/Kafka/S3 independently for tests.
- Documentation: Update API docs, configs, and operations guides per phase.

Dependencies and sequencing
- Phases are sequential gates: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7.
- Each phase has explicit performance or functional acceptance criteria to move forward.

Open items
- See open-questions.md for decisions required to finalize SLAs, limits, and exact acceptance thresholds.