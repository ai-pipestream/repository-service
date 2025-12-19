# S3 Multipart Upload (Future Upload Mode)

This document is a **reference** for implementing multipart uploads in a later phase (not Phase 1).

- Phase 1 canonical ingestion is **`PipeDocService.SavePipeDoc`** (see `../../DESIGN.md`).
- Multipart is for **very large objects** and/or **streaming** where we don’t want to hold the full payload in memory.
- Coordination state is persisted in the repository database (durable, restart-safe).

## S3 Multipart Basics

### Three-phase S3 process

1. **CreateMultipartUpload** → returns `upload_id`
2. **UploadPart** (repeat) → returns `etag` per part
3. **CompleteMultipartUpload** → finalizes object

### Constraints (S3/MinIO compatible)

- **Minimum part size**: 5MB (except last part)
- **Maximum part size**: 5GB
- **Maximum parts**: 10,000
- **Maximum object size**: 5TB
- **Part numbers**: 1..10000 (1-based)
- **Completion**: needs an ordered list of `{part_number, etag}`

## Coordination Without an External Cache/Queue

Multipart needs a place to store:

- the `s3_upload_id`
- the target bucket/key
- per-part `etag` + byte counts
- current status (`PENDING`, `UPLOADING`, `COMPLETING`, `COMPLETED`, `FAILED`, `CANCELLED`)

**Recommended approach:** persist coordination state in the repository-service database.

- **Table example (conceptual)**:
  - `upload_session(id, account_id, connector_id, doc_id, drive_id, s3_bucket, s3_key, s3_upload_id, status, created_at, updated_at, expected_size_bytes, content_type)`
  - `upload_part(session_id, part_number, etag, size_bytes, status, created_at)`

This gives:

- **Durability** across restarts
- **Idempotency** (upsert parts, dedupe repeated part uploads)
- **Queryability** (progress reporting / receipts)

## When to Use Single PUT vs Multipart

Even in the multipart implementation, you should keep a **single PUT fast path** for smaller payloads.

- Below a configurable threshold, do a simple `PutObject`
- Above it, do multipart

The exact threshold is an operational tuning knob; the design’s key requirement is correctness and the ability to handle very large objects.

## Abort/Cleanup

Multipart uploads consume resources if left incomplete.

- **Client cancellation**: call `AbortMultipartUpload` and mark the session `CANCELLED`.
- **Server cleanup**: use an S3 lifecycle rule to abort incomplete multipart uploads after N days.

## Versioning and Receipts

If the bucket has versioning enabled, store the returned `s3_version_id` after completion.

For receipts/events, prefer:

- `doc_id` (stable)
- `checksum` / content hash (version discriminator)
- `s3_bucket`, `s3_key`, optional `s3_version_id`
- `size_bytes`, timestamps

## Encryption Notes (phased)

- **Phase 1**: SSE-S3 (or SSE-KMS if supported in your MinIO setup)
- Later: client-provided/envelope encryption (see `../../DESIGN.md`)

## Related

- `../../DESIGN.md` (authoritative)
- `01-upload-flow.md` (overview of the three upload modes)
