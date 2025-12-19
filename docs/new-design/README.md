## Repository Service – Design Notes (Historical + Supporting)

This folder contains a set of design documents that were created during an earlier iteration of `repository-service`.

### Important
- The **current consolidated source of truth** is the root [`DESIGN.md`](../../DESIGN.md).
- Several files in this folder describe an **intake-first / chunked gRPC upload API** (`InitiateUpload`, `UploadChunk`, etc.). That API shape is **not the canonical plan** anymore.

### What changed vs these docs
Our current agreed design:
- **Canonical API**: `PipeDocService.SavePipeDoc` (not `InitiateUpload`/`UploadChunk`).
- **Upload modes**:
  1. Direct PipeDoc insertion (inline bytes allowed; can offload when needed)
  2. Multipart / streaming upload (bring back, but re-implemented with simpler correctness-first approach)
  3. HTTP POST streaming upload (raw bytes + secure header metadata auth) with enough metadata tagged to create a PipeDoc reference
- **Hydration/dehydration** is explicit and policy-driven (engine + repo-service cooperate).
- **Kafka**: events are lean state changes; consumers (e.g., `opensearch-manager`) call back to repo-service APIs to fetch authoritative data (no shared DB).

### How to use this folder going forward
- Treat these documents as **supporting deep dives** and **idea inventory**.
- We will progressively update/trim them so they match `DESIGN.md` (or clearly state they are historical).

### Recommended reading order (current)
1. [`../../DESIGN.md`](../../DESIGN.md) – consolidated design and decisions
2. [`phase-1-savepipedoc.md`](phase-1-savepipedoc.md) – Phase 1 canonical endpoint runbook
3. [`phases.md`](phases.md) – updated phase plan mapped to the consolidated design
4. [`04-kafka-events.md`](04-kafka-events.md) – Kafka event philosophy + callback pattern
5. [`03-s3-multipart.md`](03-s3-multipart.md) – multipart constraints (future upload mode reference)

### Documents in this folder
- `00-overview.md`: historical overview (will be updated to match DESIGN)
- `01-upload-flow.md`: historical chunked gRPC flow (will be updated to include the 3 upload modes)
- `03-s3-multipart.md`: multipart constraints, encryption, operational notes (mostly reusable)
- `04-kafka-events.md`: event design + callback pattern (mostly reusable; remove outdated topic assumptions)
- `phase-1-savepipedoc.md`: Phase 1 runbook for the canonical endpoint
- `06-implementation-plan.md`: historical 10-week plan (will be updated to match new phases)
- `open-questions.md`: historical Q&A; many answers are now captured in `DESIGN.md`
