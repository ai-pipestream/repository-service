## Repository Service – Updated Architecture Overview

This overview is aligned with the consolidated design in [`../../DESIGN.md`](../../DESIGN.md).

### Purpose
`repository-service` is the platform’s **durable librarian**:
- Stores document bytes in **S3-compatible object storage** (MinIO in dev/test).
- Stores authoritative metadata + version history in the **repo-service database**.
- Emits **Kafka state-change events** so other services can build derived indexes without sharing databases.

### Cooperation model
- **connector-intake-service** (gateway) can normalize uploads and choose the best upload mode.
- **engine** (future) may run fast gRPC-to-gRPC hops without persistence, but must persist when using Kafka hops.
- **parser modules** hydrate binaries for parsing; after parsing, binaries are typically dropped or kept as references.

### Three upload modes
We support three upload paths that converge on the same internal concepts (“store bytes”, “store metadata”, “emit event”).

1) **Direct PipeDoc insert**
- Client/intake sends a PipeDoc directly.
- Inline bytes are allowed for convenience.
- Large payloads can be offloaded to S3 and referenced via `Blob.storage_ref`.

2) **Multipart / streaming upload**
- Used for large files.
- Implementation may use multipart upload and optional coordination layers, but the API contract is focused on correctness, resilience, and receipts.

3) **HTTP POST stream**
- Client streams raw bytes over HTTP.
- Uses **secure header metadata auth**.
- Bytes are stored “as-is” to S3 with enough metadata tagged to construct a PipeDoc reference.

### Hydration & dehydration
Hydration/dehydration is policy-driven:
- **Hydrate**: turn `storage_ref` into inline bytes when needed.
- **Dehydrate**: store inline bytes and replace them with a `storage_ref`.

Default engine policy after parsing:
- drop inline binary bytes
- keep a storage reference so the original is retrievable

Additional supported use case:
- binary retained inline between two parser hops (parser1 → parser2), then dropped/ref’d.

### Event-driven architecture
Kafka events are **lean** and represent state changes. Consumers should use the **callback pattern**:
- Consume event
- Call repo-service API to fetch authoritative metadata
- Index/store derived state

This avoids race conditions and avoids shared databases.

### Storage model (authoritative stores)
- **DB**: drives, nodes, document identities, versions, metadata
- **S3**: raw bytes / protobuf payloads
- **Index (OpenSearch via opensearch-manager)**: derived “collections” search for repo-service browsing

For details on lifecycle, idempotency, encryption, and subscribers, see [`../../DESIGN.md`](../../DESIGN.md).
