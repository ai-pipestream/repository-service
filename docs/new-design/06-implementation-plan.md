## Implementation Plan (Updated)

This plan is aligned with the consolidated design in [`../../DESIGN.md`](../../DESIGN.md) and the updated phase plan in [`phases.md`](phases.md).

> Note: This file previously contained a 10-week plan centered around a chunked gRPC upload API (`InitiateUpload`/`UploadChunk`) and a queue-based chunk coordinator. That approach is now treated as an optional implementation detail for multipart uploads, not the canonical starting point.

---

## Phase 0: Baseline
**Goal**: build green + health endpoints.

**Deliverables**
- `./gradlew test` passes
- service starts in dev
- basic health endpoints return UP

---

## Phase 1: Canonical persistence (PipeDocService)
**Goal**: implement the canonical repo-service write/read flow.

### Scope
- Implement `PipeDocService.SavePipeDoc`
  - validate drive + connector
  - accept inline blobs and/or storage references
  - persist authoritative metadata + version rows in DB
  - store bytes in S3 when needed
  - emit Kafka `RepositoryEvent` (Created/Updated)
- Implement `PipeDocService.GetPipeDoc` (and minimal listing needed by indexers)
- Implement minimal hydration/dehydration behavior for blobs
  - store and return `Blob.storage_ref` for large or durable flows

### Tests
- 1 integration test proving the full loop:
  - SavePipeDoc → DB commit → bytes persisted or referenced → event emitted → GetPipeDoc returns

---

## Phase 2: Multipart / streaming upload
**Goal**: support very large file uploads efficiently.

### Scope
- Add an upload session + multipart workflow
- Confirmed progress updates
- Cleanup of abandoned uploads

### Notes
- Multipart coordination must not depend on any external cache/queue; use DB + S3 primitives and/or in-process coordination as needed.
- Prefer correctness and operational simplicity.

---

## Phase 3: HTTP POST upload
**Goal**: support bulk byte ingest via HTTP.

### Scope
- HTTP POST stream raw bytes with **secure header metadata auth**
- Store bytes in S3 “as-is” and tag object metadata
- Construct a PipeDoc reference and persist canonical metadata
- Emit Kafka state-change events

---

## Phase 4+: Hydration/dehydration enforcement hooks
**Goal**: make payload management consistent for engine/module hops.

### Scope
- Policy-driven hydration/dehydration
- Support the special parser-to-parser case (retain binary inline between two parsers, then drop/ref)

---

## Phase 5: Hardening
- authN/Z evolution
- encryption phases
- ops/runbooks
- quotas/limits
- dashboards and alerting
