# Phase 2 Runbook — Throughput Validation (intake → repository-service)

This document defines how we prove high ingest rates between the connector-intake-service and repository-service before introducing Redis/Kafka/S3 persistence costs. The output of this phase is a reproducible benchmark report with tuning recommendations.

Status: Ready to execute after Phase 1 handlers are in place

## Objectives

- Drive high-concurrency UploadChunk traffic from intake to repository-service.
- Measure end-to-end RPC latency and sustained throughput for various chunk sizes and concurrency levels.
- Identify bottlenecks (CPU, memory, Netty/gRPC, JVM GC, NIC, OS limits) and recommend tuning.

## Test Topology

- Client: connector-intake-service (preferred) acting as the sender to repository-service.
  - Alternatively, use a simple standalone benchmark client that directly calls repository-service RPCs (fallback).
- Server under test (SUT): repository-service with Phase 1 fast-ACK enabled (no persistence).
- Single-node and multi-node variants if available. Start with single node.

## Workload Profiles

Run the matrix below (you can subset initially, then expand):

- Chunk sizes (bytes): 1MB, 4MB, 8MB, 16MB, 32MB, 64MB, 128MB
- Concurrency per client: 8, 16, 32, 64, 128, 256
- Total payload volume per run: ≥ 50 GB (or ≥ 10 minutes sustained)
- Ordering: randomized chunk_number order
- Retries: enable client retries with backoff

Record the full configuration used per run.

## Metrics to Collect

- From repository-service
  - RPC latencies: `rpc_upload_chunk_ack_latency_ms` (p50/p95/p99)
  - QPS: `upload_chunk_received_total` rate
  - Payload dist: `upload_chunk_payload_bytes`
  - CPU%, memory RSS, heap usage, GC pause time (JFR or Micrometer JVM metrics)
  - Netty/gRPC server thread pool utilization

- From connector-intake-service (client)
  - Send rate (chunks/sec, MB/s)
  - Client-side latencies and error rates (deadline exceeded, unavailables)

- System-level
  - NIC throughput (Gbps)
  - OS TCP metrics (SYN backlog, retransmits)

## Tuning Knobs

Apply minimally necessary tuning; document every change:

- gRPC/Netty (repository-service)
  - `quarkus.grpc.server.max-inbound-message-size` (raise for large chunks)
  - Thread pools: event loop threads (`quarkus.vertx.event-loops-pool-size`) and worker threads
  - Flow control: HTTP/2 window sizes (if exposed), keepalive pings

- gRPC/Netty (client side)
  - Max outbound message size
  - Channel count and event loop threads

- JVM
  - `-Xms`/`-Xmx` sized to avoid GC pressure (e.g., 1–2x payload concurrency windows)
  - GC: G1 defaults are fine initially; record pauses. Optionally test ZGC on JDK 17+ if available

- OS/container
  - `ulimit -n` file descriptors
  - `net.core.somaxconn`, `net.ipv4.ip_local_port_range`, `net.ipv4.tcp_tw_reuse`
  - Container CPU/memory limits

## Running the Tests

1) Build and run repository-service with Phase 1 enabled

```
./gradlew build
QUARKUS_PROFILE=bench ./gradlew quarkusDev
```

Example `application-bench.properties` overrides:

```
repo.features.phase1.enabled=true
quarkus.grpc.server.max-inbound-message-size=268435456
quarkus.vertx.event-loops-pool-size=8
quarkus.thread-pool.core-threads=64
quarkus.thread-pool.max-threads=256
quarkus.micrometer.export.prometheus.path=/q/metrics
```

2) Start connector-intake-service in a matching bench profile (or use the standalone load tool). Configure:
- Target repository-service host:port
- Concurrency and chunk size
- Total data volume and random payload source (zero-copy when possible)

3) Execute workload matrix. For each run:
- Warm up for 60s; measure for 10 minutes
- Capture Prometheus snapshots and logs
- Note any errors and retry counts

## Success Criteria (Gate)

Because targets are environment-specific, use these qualitative gates now and set quantitative thresholds once data is gathered:

- Stability: error rate <= 0.1% (timeouts/retries) during sustained load
- Latency: UploadChunk p95 stable and not linearly degrading with moderate concurrency
- Throughput: clear scaling trend with concurrency up to NIC/CPU saturation; document the knee point

After the first pass, pick at least two representative configurations (e.g., 16MB x 64 conc and 32MB x 128 conc) and set concrete pass/fail thresholds based on p95 latency and sustained MB/s observed.

## Instrumentation and Dashboards

- Enable Prometheus scrape for both services
- Provide a Grafana dashboard with panels:
  - UploadChunk latency (p50/p95/p99)
  - QPS and MB/s
  - JVM heap, GC pause time
  - CPU, RSS
  - Netty event loop queue length
  - Error rates by code

Export dashboard JSON in `docs/bench/grafana/phase2.json` (optional).

## Reporting Template

Create `docs/new-design/reports/phase-2-benchmark-<date>.md` including:

- Environment
  - Hardware (CPU cores, RAM)
  - Network (10GbE/2.5GbE), OS and kernel
  - JVM version and container limits
- Service configs (both client and server)
- Workload matrix table
- Results: throughput (MB/s), p95/p99 latency, error rates
- Observations: where saturation occurs, bottlenecks found
- Tuning applied and their effects
- Recommendations for defaults going into Phase 3+

## Risks & Considerations

- Very large chunk sizes may hit gRPC message limits—document limits and enforce maximums as needed
- Client-side backoff behavior can mask server issues; capture both sides
- Avoid noisy neighbors by isolating test nodes where possible

## Next Phase Handoff

Use the findings to:
- Set sane defaults for concurrency, chunk size hints, and message limits
- Size Redis/Kafka/S3 thread pools in later phases
- Finalize concrete pass/fail thresholds for regression tracking
