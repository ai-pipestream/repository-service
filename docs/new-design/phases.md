## Phases (incremental, testable milestones)

This phase plan is a companion to the root [`DESIGN.md`](../../DESIGN.md). It replaces the earlier chunked gRPC-centric phase plan in this folder.

### Phase 0: Baseline build + health
- **Goal**: Service builds, runs locally, and health endpoints work.
- **DOD**: `./gradlew test` passes; `/q/health` is UP in dev.

### Phase 1: Canonical document persistence (PipeDocService)
- **Goal**: Implement the canonical write/read path for the repository.
- **Scope**:
  - Implement **`PipeDocService.SavePipeDoc`** end-to-end.
  - Persist bytes to S3-compatible storage **or** accept inline bytes.
  - Persist authoritative metadata + version rows in DB.
  - Emit **lean Kafka state-change events** (`RepositoryEvent.Created/Updated/Deleted`) with callback pattern.
  - Implement **`PipeDocService.GetPipeDoc`** and basic listing / metadata fetch needed by indexers.
- **DOD**:
  - Integration test proves: SavePipeDoc → DB row committed → bytes stored (or referenced) → event emitted → GetPipeDoc returns.

### Phase 2: Multipart/streaming upload (bulk bytes)
- **Goal**: Support large file uploads efficiently.
- **Scope**:
  - Reintroduce multipart/streaming upload with a correctness-first approach.
  - Support confirmed progress updates (not necessarily byte-perfect live streaming initially).
  - Provide a finalize step that produces a PipeDoc referencing the stored blob.
- **Notes**:
  - Multipart coordination must not depend on any external cache/queue. Use DB/S3 primitives and/or in-process coordination as needed.

### Phase 3: HTTP POST streaming upload
- **Goal**: Provide the most practical bulk-ingest path.
- **Scope**:
  - HTTP POST stream raw bytes to repo-service.
  - Authenticate via secure header metadata.
  - Store bytes “as-is” in S3 with metadata tags sufficient to create a PipeDoc reference.
  - Return a receipt and emit events.

### Phase 4: Hydration/dehydration enforcement hooks
- **Goal**: Make payload management consistent and predictable across engine/module hops.
- **Scope**:
  - Implement repo-service APIs to hydrate/dehydrate blobs based on policy.
  - Support the special case: binary retained inline between two parsers (parser1→parser2), then dropped/ref’d.

### Phase 5: Hardening + ops
- **Goal**: Production readiness.
- **Scope**:
  - retry/backoff, cleanup of abandoned multipart uploads, quotas/limits
  - metrics/tracing/logging dashboards
  - authN/authZ evolution
  - encryption phases (SSE-S3/KMS first; client-provided key/envelope next; streaming crypto later)

---

### Notes on legacy documents in this folder
Several existing docs still describe the earlier “InitiateUpload / UploadChunk” plan. Treat those as historical reference unless/until updated to match the above phases.
