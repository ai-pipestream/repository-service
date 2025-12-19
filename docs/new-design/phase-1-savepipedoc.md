## Phase 1 Runbook: `PipeDocService.SavePipeDoc`

Phase 1 goal: implement the **canonical** repo ingestion path: **Save a PipeDoc, persist it, emit a lean event, and return a receipt**.

Authoritative design: `../../DESIGN.md`

### Scope (Phase 1)

- **Endpoint**: `PipeDocService.SavePipeDoc`
- **Storage**:
  - Persist PipeDoc metadata in DB
  - Support claim-check pattern for large blobs (store bytes in S3 and keep a `storage_ref`)
- **Events**: emit a lean `RepositoryEvent` on successful commit
- **Idempotency**:
  - Stable identity: `(account_id, connector_id, doc_id)`
  - Version discriminator: `checksum`

### Non-goals (Phase 1)

- Multipart upload API
- HTTP POST streaming upload
- Advanced hydration policy engine (beyond minimal “inline vs storage_ref” handling)
- Strong authN/authZ enforcement (beyond plumbing `SourceContext` and placeholders)

### Request (what callers should send)

- `account_id` / `connector_id` (directly or via `SourceContext` depending on proto)
- `PipeDoc` containing:
  - `doc_id` (client-provided)
  - `checksum` (content hash/version key)
  - blobs either inline (small) or referenced (for claim-check)
- Optional drive selection, otherwise repo-service uses the account’s configured default drive.

### Server-side steps

1. **Validate**
   - required IDs, doc_id present
   - checksum present (or compute later; Phase 1 can accept caller-provided)
2. **Resolve drive**
   - look up default drive for the account (must exist)
3. **Dehydrate if needed**
   - if any blob bytes exceed policy/threshold for durable storage, store bytes in S3 and replace with `storage_ref`
4. **Persist**
   - write PipeDoc record(s) and blob metadata within a DB transaction
5. **Emit event**
   - after commit, emit a lean `RepositoryEvent` to `repository.events`
6. **Return receipt**
   - include `doc_id`, `checksum`, drive, any S3 refs/version ids, and a coarse status.

### Receipts

Phase 1 keeps receipts simple and future-proof:

- **PipeDoc receipt**: `(doc_id, checksum, status, timestamps)`
- **Blob receipt(s)** (when stored): `(blob_id, s3_bucket, s3_key, optional s3_version_id, size_bytes)`

### Testing checklist

- SavePipeDoc persists and returns receipt
- Idempotent retry with same `(doc_id, checksum)` does not create duplicates
- Large blob triggers S3 offload + storage_ref replacement
- Event emitted on success (and not emitted on failed transaction)

### Links

- `../../DESIGN.md`
- `04-kafka-events.md`
- `03-s3-multipart.md` (future)

