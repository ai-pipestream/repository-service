# Repository Service - New Architecture Overview

## Purpose

The Repository Service is responsible for storing and retrieving documents in a distributed, scalable manner using Redis for temporary chunk queuing and S3 for persistent storage.

## Key Design Principles

### 1. Intake-First Pattern
The service now follows an **intake-first pattern** where:
- First call returns metadata (node ID, upload ID) immediately
- Client can then send chunks in parallel without waiting
- Service returns status synchronously so clients can poll or stream progress

### 2. Simplified Input Interface
Previous versions had overly complex inputs. The new design:
- **InitiateUpload**: Returns node_id and upload_id with minimal metadata
- **UploadChunk**: Unary calls (not streaming) to enable true parallel uploads
- **GetUploadStatus**: Polling interface for status tracking
- **StreamUploadProgress**: Server-streaming for real-time updates

### 3. Two-Phase Upload Process

#### Phase 1: Metadata Registration (Blocking)
```
Client → InitiateUpload(metadata) → Server
Server responds with:
  - node_id (UUID for the document)
  - upload_id (for tracking this specific upload)
  - created_at (timestamp)
```

**What happens internally:**
- Generate node_id (or use client-provided client_node_id)
- Create upload tracking entry in Redis
- Initiate S3 multipart upload (get upload_id from S3)
- Store upload state in Redis with TTL (24 hours default)
- Return immediately - no S3 upload happens yet

#### Phase 2: Parallel Chunk Upload (Non-blocking)
```
Client sends chunks in parallel:
  UploadChunk(node_id, upload_id, chunk_1, chunk_number=0)
  UploadChunk(node_id, upload_id, chunk_2, chunk_number=1)
  UploadChunk(node_id, upload_id, chunk_3, chunk_number=2, is_last=true)

Each chunk:
  - Stored in Redis queue: "upload:{upload_id}:chunks:{chunk_number}"
  - Triggers async S3 part upload
  - Returns immediately with acknowledgment
```

**What happens internally:**
- Each chunk is queued in Redis with metadata
- Background workers process chunks → upload to S3 as multipart chunks
- ETags are stored in Redis as they complete
- When all chunks uploaded + all ETags present → complete S3 multipart upload

### 4. Redis as Coordination Layer

Redis serves multiple purposes:
1. **Chunk Queue**: Temporary storage for incoming chunks
2. **Upload State**: Track progress (chunks received, ETags collected)
3. **Metadata Cache**: Store file hash, size, completion status
4. **ETag Collection**: Ordered list of S3 part ETags

**Data structures:**
```
upload:{upload_id}:state          → Hash (status, total_chunks, received_chunks, etc.)
upload:{upload_id}:chunks:{n}     → String (binary chunk data) + TTL
upload:{upload_id}:etags          → Sorted Set (chunk_number → etag)
upload:{upload_id}:metadata       → Hash (sha256, size, mime_type, etc.)
```

### 5. S3 Multipart Upload Coordination

**Key insight:** S3 multipart uploads require:
1. Initiate multipart upload → get upload_id
2. Upload parts (min 5MB except last part) → get ETags
3. Complete multipart upload with ordered list of ETags

**Our approach:**
- Phase 1: Initiate S3 multipart upload
- Phase 2: Upload chunks as S3 parts (parallel), collect ETags in Redis
- Background worker: When all ETags collected → complete S3 multipart upload

**Chunk size flexibility:**
- **Single chunk**: If total size < 100MB → direct PUT to S3, return immediately
- **Multiple chunks**: Use multipart upload with 5MB minimum (except last chunk)
- **Client decides**: Client controls chunk size based on network conditions

### 6. Event-Driven Architecture

**Kafka events** are published at key points:
1. **Upload initiated**: `UploadInitiated` event with node_id
2. **Chunk uploaded**: `ChunkUploaded` event (optional, for monitoring)
3. **Upload completed**: `UploadCompleted` event with s3_key, etag, hash
4. **Upload failed**: `UploadFailed` event with error details

Events are consumed by:
- **OpenSearch indexing service**: Index document metadata
- **Analytics service**: Track upload metrics
- **Notification service**: Alert users of completion
- **Repository service itself**: Update internal state, trigger post-processing

## Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                                │
│  (Connector, UI, CLI)                                        │
└───────────────┬─────────────────────────────────────────────┘
                │
                │ gRPC (InitiateUpload)
                ▼
┌─────────────────────────────────────────────────────────────┐
│              Repository Service (Phase 1)                    │
│  - Generate node_id, upload_id                               │
│  - Initiate S3 multipart upload                              │
│  - Create Redis tracking entry                               │
│  - Return metadata immediately                               │
└───────────────┬─────────────────────────────────────────────┘
                │
                │ Response: {node_id, upload_id}
                ▼
         Client (Phase 2)
                │
                │ gRPC (UploadChunk × N in parallel)
                ▼
┌─────────────────────────────────────────────────────────────┐
│              Repository Service (Phase 2)                    │
│  - Queue chunk in Redis                                      │
│  - Return acknowledgment                                     │
│  - Trigger async S3 upload                                   │
└───────────────┬─────────────────────────────────────────────┘
                │
                ├─────► Redis (Chunk Queue)
                │       upload:{id}:chunks:{n}
                │       upload:{id}:etags
                │       upload:{id}:state
                │
                └─────► Background Workers
                        │
                        ├─────► S3 (Multipart Upload)
                        │       - Upload part N
                        │       - Store ETag in Redis
                        │       - When all parts done → Complete upload
                        │
                        └─────► Kafka
                                - Publish UploadCompleted event
                                - Consumers index/process
```

## Technology Stack

- **gRPC**: Service interface (defined in platform-libraries protos)
- **Redis**: Chunk queuing, state tracking, ETag collection
- **S3/MinIO**: Persistent object storage
- **Kafka**: Event streaming for async processing
- **MySQL**: Metadata storage (node records, drive records)
- **OpenSearch**: Full-text search and analytics (via Kafka consumers)

## Comparison with Old Architecture

| Aspect | Old Design | New Design |
|--------|-----------|------------|
| **Upload pattern** | Complex streaming with backpressure | Simple unary calls, true parallel |
| **Metadata return** | After full upload | Immediately after Phase 1 |
| **Chunk coordination** | In-memory maps | Redis with persistence |
| **S3 interaction** | Direct, synchronous | Queued, async workers |
| **Scalability** | Single instance | Horizontally scalable |
| **Failure recovery** | Lost on restart | Redis persistence survives restarts |
| **Progress tracking** | Limited | Full Redis-backed state |

## Benefits

1. **Fast Response**: Clients get node_id immediately, don't wait for upload
2. **True Parallelism**: Unary UploadChunk calls enable client-side parallelism
3. **Resilience**: Redis persistence survives service restarts
4. **Scalability**: Multiple service instances share Redis queue
5. **Monitoring**: Clear state in Redis for observability
6. **Flexibility**: Supports small single-chunk and large multi-chunk uploads
7. **Event-Driven**: Kafka events enable loose coupling with consumers

## Next Steps

See the following documents for detailed designs:
- [01-upload-flow.md](01-upload-flow.md) - Detailed upload flow diagrams
- [02-redis-queuing.md](02-redis-queuing.md) - Redis data structures and queuing
- [03-s3-multipart.md](03-s3-multipart.md) - S3 multipart upload coordination
- [04-kafka-events.md](04-kafka-events.md) - Kafka event patterns
- [05-comparison.md](05-comparison.md) - Detailed comparison with legacy
- [06-implementation-plan.md](06-implementation-plan.md) - Implementation roadmap
