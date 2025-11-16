# Open Questions (for user confirmation)

This list captures decisions and clarifications needed to proceed efficiently. Please answer inline or mark decisions so we can lock interfaces and SLAs.

## 1) Intake API and Chunking
- Min chunk size policy: Do we enforce a minimum chunk size (e.g., 5–16 MB) on `UploadChunk`, or accept any size and adjust server-side? Impact: S3 multipart requires ≥5 MB for non-final parts.
  - we should adjust any size.  I know the initial spec said to make sure it's consistent, etc.. but it really can be any size and have no affect on the outcome.  As long as it works..
- Max chunk size: What is the practical upper bound per chunk (e.g., 64–128 MB) to balance memory, concurrency, and throughput?
  - We don't know and there could be cases for larger chunk sizes.. we also plan to scale this (at first I'll use fargate).. so we expect some limits to pause it.. 
- Max concurrent chunks per upload from a single client: Do we set an explicit cap (e.g., 8–32) to prevent hotspotting? Server should advertise limits to clients?
  - probably not?  I don't mind if a client sends a ton at one time, that is what we want right?
- Ordering requirements: Do we strictly require in-order arrival of chunks or allow out-of-order and rely on `chunk_number` + idempotency to reconcile?
  - out of order!! Totally allowed! We keep track of the chunk number, get the tag from s3, and we send the ordering of the chunk tags to s3 at the end.. right?
- Idempotency: Will clients retry `UploadChunk` with the same `chunk_number` and identical bytes? If a duplicate is detected, should we return OK without re-queuing/uploading?
  - yes but maybe we can have a force boolean in case there's a reason for it?
- Checksums: Which checksum(s) are required on `UploadChunk`? Options: sha256-per-chunk, overall sha256 at completion, both, or none. If provided, is server authoritative on verification?
  - Not sure if we even need to enforece it?  We can calculate it for the client if it's not there.
- Metadata on InitiateUpload: Minimal set is currently unspecified beyond node/upload IDs. Confirm fields: filename, mime_type, expected_size, content_hash, drive_id, retention_class, encryption class, tags?
  - expected_size isn't needed.. let's start by KISS principle 

## 2) Performance Targets (throughput and latency)
- Target sustained ingest throughput from intake → repository service (single node and cluster): e.g., 1–5 GB/s per node? Aggregate target across N nodes?
  - not sure what we would define.  we have a 10gb internal network with a lot of machines/ raspberry pis on nvme with 2.5gb connection - we may want to make a cluster to test?  But as long as it works at first so we can scale it.. we can assume redis is fast and s3 is slow, which is the point of doing this the way we are.
- Target p50/p95/p99 latencies for `InitiateUpload` and `UploadChunk` RPCs? E.g., p95 < 20 ms for UploadChunk ACK at 16 MB chunk?
  - we should capture them via metrics to see what we want to make as a target.  Each environment will be unique in speed.
- Max object size supported (TB-scale?) and expected typical size distribution (e.g., 1 MB – 5 GB)?
  - I feel like whatever the host machine can support.  It's HIGHLY unlikely that we would have many candidates in the multi-gb range .. if we do then we can treat as one-offs I guess?  But if it can handle larger ones, we should do it... 
- Resource envelopes for a single repository-service pod: CPU cores, RAM, network bandwidth to plan buffers and concurrency defaults.
  - not sure
- Back-pressure behavior when Redis/Kafka/S3 slows down: Should intake start rejecting (fast-fail) or buffer up to bounded memory/Redis capacity with 429s when exceeded?
  - not sure.. 

## 3) Redis Design & Limits
- Redis deployment mode for prod: single primary-replica vs. clustered? Required persistence (AOF + RDB)?
  - I'm not familiar to be honest...
- Memory limits and eviction: What total memory budget and eviction policy (noeviction vs. volatile-ttl)? What TTLs for chunk keys and state hashes?
  - not sure
- Key naming and tenancy: Multi-tenant isolation requirements (per-tenant prefixes/limits/quotas)?
  - not sure
- Reliability expectation: Is losing unprocessed chunks acceptable under worst-case Redis loss, or must we guarantee no-loss via Kafka-before-Redis or write-through to S3 temp?
  - sounds overkill.. redis is going to be used as our cache layer.  I think if it gets overwhelmed, we should just block new requests, right?

## 4) Kafka Events
- Topics and partitioning strategy: Partition by `node_id`, `upload_id`, or tenant? Target partitions count initially?
  - It should be by node id- so we know it's i norder right?
- Required events: Are `ChunkUploaded` events mandatory or only `UploadInitiated/Completed/Failed`? Event volume vs. observability trade-off.
  - is it a big overhead to do each chunk?  I feel like kafka can handle a load like that just fine.  We're talkin like 2K per message, right?
- Schema registry: Avro + Apicurio confirmed? Evolution policy (backward compatible only)?
  - yes.. it is PROTOBUF + APICURIO.. there should be NO MENTION OF AVRO
- Delivery guarantees: At-least-once sufficient? Do we need exactly-once semantics anywhere (likely no)?
  - probably not

## 5) S3/MinIO Behavior
- Storage class default: STANDARD vs. cost-optimized class? Need per-upload override?
  - clients will provide their s3 credentials, minio is mainly for local development but we'll want to try for whatever s3 capable storage is there.. 
- Server-side encryption: Default to SSE-S3? Do we need SSE-KMS or SSE-C; if KMS, specify key IDs/tenancy mapping.
  - Not sure.. 
- Multipart minimum part size: Use AWS 5 MB minimum for non-final parts; do we enforce client-side or adjust by buffering server-side (prefer not to)?
  - I don't think we need to buffer server-side.. but the client can easily send variable sizes right?
- Bucket layout and key scheme: Proposed `tenant/{drive_id}/{node_id}` – confirm or provide preferred layout. Any max objects per prefix concerns?
  - not sure - are there?
- Large single-part optimization: Threshold for direct PUT vs. multipart (e.g., < 100 MB direct PUT) – confirm.
  - not sure

## 6) Data Model & Metadata
- Source of truth for node metadata: MySQL vs. Redis vs. S3 object metadata? Current design suggests MySQL for durable metadata; confirm fields and write timing.
  - is that wise?
- Unique constraints: Uniqueness on `(drive_id, path)` or `(tenant_id, external_id)`? Any dedup requirements (hash-based)?
  - sounds like we need to discuss more.. the idea I had was that each of these will end in a reference to be carried byu the PIpestream in the engine when we go from one step to the next and it's not grpc.  if it's grpc, we would just send the entire doc 
- Retention & legal hold: Do we need WORM/retention locks at object level (MinIO supports) in MVP?
  - nah

## 7) Reliability & Recovery
- SLA/SLOs: Availability target for ingest (e.g., 99.9%) and durability target for stored objects.
- Crash recovery: On service restart, do we re-scan Redis to resume incomplete uploads automatically? Max resume window (TTL)?
- Partial failures: If some parts succeed to S3 and others fail, is it acceptable to abort multipart and retry entire upload, or should we retry only failed parts until a policy limit?

## 8) Security & AuthN/Z
- Client auth: mTLS, OAuth2/JWT, or internal-only (service mesh)? For MVP, do we rely on sidecar or terminate TLS in service?
- Authorization: Is there a concept of tenant/project/drive scoping enforced at the gRPC layer? Provide claims/headers mapping.
- PII/PHI constraints: Any encryption or data masking requirements at-rest or in-flight beyond TLS/SSE?

## 9) Observability
- Metrics: Confirm must-have metrics (ingest QPS, chunk sizes, Redis queue depth, S3 part durations, E2E commit latency, error rates, retries).
- Tracing: OpenTelemetry enabled with gRPC, Redis, S3, Kafka spans? Sampling rates?
- Logging: JSON logs with correlation IDs (`upload_id`, `node_id`) – any additional IDs?

## 10) Operations
- Environments: dev, staging, prod – any others? Promotion flow?
- Rollouts: Blue/green or canary requirements? Backward compatibility across versions for in-flight uploads.
- Cost controls: Limits per tenant (rate limiting, quotas) – MVP or later phase?

## 11) Throughput Test Plan (Phase 2)
- Target hardware for the test: single VM specs and network? Cluster size?
- Test harness: Will intake-service be used as the load generator, or should we provide a standalone benchmark tool?
- Pass/fail criteria: Define minimum sustained throughput and acceptable error rate for proceeding to persistence phases.

## 12) Timelines & Priorities
- Feature gate sequence: Confirm we start with intake-only + ack path, then performance test, then Redis/Kafka, then S3 multipart, then full lifecycle.
- Hard date constraints or external dependencies we must align with?

Please review and annotate with decisions; we’ll codify them into the implementation plan and configs.
