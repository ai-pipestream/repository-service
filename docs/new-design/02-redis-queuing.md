# Redis Queuing Strategy

## Overview

Redis serves as the coordination layer for chunk uploads, providing:
1. **Temporary chunk storage** (before S3 upload)
2. **Upload state tracking** (progress, status)
3. **ETag collection** (ordered list for S3 multipart completion)
4. **Worker coordination** (pub/sub for async processing)
5. **Idempotency** (prevent duplicate chunk processing)

## Data Structures

### 1. Upload State (Hash)

Stores the current state of an upload.

```redis
Key: upload:{upload_id}:state
Type: Hash
TTL: 86400 seconds (24 hours)

Fields:
  node_id              → UUID of the document
  s3_upload_id         → S3 multipart upload ID
  s3_key               → Target S3 key (e.g., "connectors/{connector_id}/{node_id}.pb")
  drive                → Drive name
  status               → PENDING | UPLOADING | COMPLETING | COMPLETED | FAILED | EXPIRED
  expected_size        → Total file size in bytes
  received_chunks      → Number of chunks received from client
  uploaded_chunks      → Number of chunks successfully uploaded to S3
  total_chunks         → Total expected chunks (0 if unknown)
  is_last_received     → true when chunk with is_last=true received
  created_at           → Unix timestamp (milliseconds)
  updated_at           → Unix timestamp (milliseconds)
  mime_type            → MIME type
  final_etag           → S3 ETag when completed
  final_size           → Actual final size when completed
  error_message        → Error details if status=FAILED
```

**Example:**
```redis
HSET upload:abc123:state \
  node_id "550e8400-e29b-41d4-a716-446655440000" \
  s3_upload_id "s3-upload-xyz789" \
  s3_key "connectors/my-connector/550e8400-e29b-41d4-a716-446655440000.pb" \
  drive "documents" \
  status "UPLOADING" \
  expected_size "1073741824" \
  received_chunks "5" \
  uploaded_chunks "3" \
  total_chunks "10" \
  is_last_received "false" \
  created_at "1700000000000" \
  updated_at "1700000010000" \
  mime_type "application/pdf"

EXPIRE upload:abc123:state 86400
```

### 2. Chunk Data (String)

Temporary storage for chunk binary data.

```redis
Key: upload:{upload_id}:chunks:{chunk_number}
Type: String (binary-safe)
TTL: 3600 seconds (1 hour)

Value: Raw binary chunk data
```

**Why separate keys per chunk?**
- Enables parallel access (multiple workers can GET different chunks)
- Individual chunk TTL (auto-cleanup after 1 hour)
- Easy to check if chunk exists (EXISTS command)
- Can delete individual chunks after S3 upload

**Example:**
```redis
# Store chunk 0
SET upload:abc123:chunks:0 <binary data>
EXPIRE upload:abc123:chunks:0 3600

# Store chunk 1
SET upload:abc123:chunks:1 <binary data>
EXPIRE upload:abc123:chunks:1 3600

# Worker retrieves chunk
data = GET upload:abc123:chunks:0

# Worker deletes after S3 upload
DEL upload:abc123:chunks:0
```

### 3. ETags (Sorted Set)

Stores S3 part ETags in chunk number order.

```redis
Key: upload:{upload_id}:etags
Type: Sorted Set
TTL: 86400 seconds (24 hours)

Score: chunk_number (0-based)
Member: S3 ETag (quoted string, e.g., "\"abc123def456\"")
```

**Why Sorted Set?**
- S3 CompleteMultipartUpload requires ETags in part number order
- ZRANGE gives ordered list automatically
- Can check if all ETags present (ZCARD == total_chunks)
- Atomic operations (ZADD is atomic)

**Example:**
```redis
# Worker adds ETags as chunks complete (order doesn't matter)
ZADD upload:abc123:etags 0 "\"etag-chunk-0\""
ZADD upload:abc123:etags 1 "\"etag-chunk-1\""
ZADD upload:abc123:etags 2 "\"etag-chunk-2\""

# Retrieve ordered ETags for S3 completion
ZRANGE upload:abc123:etags 0 -1
# Returns: ["\"etag-chunk-0\"", "\"etag-chunk-1\"", "\"etag-chunk-2\""]

# Check if all parts uploaded
ZCARD upload:abc123:etags  # Returns: 3
```

### 4. Upload Metadata (Hash)

Stores file metadata for final processing.

```redis
Key: upload:{upload_id}:metadata
Type: Hash
TTL: 86400 seconds (24 hours)

Fields:
  name                 → Original filename
  connector_id         → Connector identifier
  parent_id            → Parent folder UUID
  path                 → Relative path (e.g., "docs/2024/")
  custom_metadata      → JSON string of custom metadata
  client_node_id       → Client-provided stable ID (if any)
```

**Example:**
```redis
HSET upload:abc123:metadata \
  name "document.pdf" \
  connector_id "my-connector" \
  parent_id "parent-folder-uuid" \
  path "docs/2024/" \
  custom_metadata "{\"author\": \"John Doe\", \"tags\": [\"important\"]}"

EXPIRE upload:abc123:metadata 86400
```

## Pub/Sub Channels

### 1. Chunk Received Notifications

When a chunk is queued, notify workers to process it.

```redis
Channel: upload:chunk:received

Message format (JSON):
{
  "upload_id": "abc123",
  "chunk_number": 5,
  "is_last": false,
  "timestamp": 1700000000000
}
```

**Usage:**
```redis
# Service publishes when chunk queued
PUBLISH upload:chunk:received '{"upload_id":"abc123","chunk_number":5,"is_last":false}'

# Workers subscribe
SUBSCRIBE upload:chunk:received
```

**Worker pattern:**
```python
# Worker subscribes to channel
for message in redis.subscribe('upload:chunk:received'):
    data = json.loads(message)
    upload_id = data['upload_id']
    chunk_number = data['chunk_number']

    # Try to claim the chunk (GETDEL is atomic)
    chunk_data = redis.getdel(f'upload:{upload_id}:chunks:{chunk_number}')

    if chunk_data is None:
        # Another worker claimed it already
        continue

    # Process chunk: upload to S3, store ETag
    etag = upload_to_s3(chunk_data, upload_id, chunk_number)
    redis.zadd(f'upload:{upload_id}:etags', {etag: chunk_number})
    redis.hincrby(f'upload:{upload_id}:state', 'uploaded_chunks', 1)

    # Check if upload complete
    check_and_complete_upload(upload_id)
```

### 2. Progress Updates

Stream real-time progress to clients.

```redis
Channel: upload:progress:{upload_id}

Message format (JSON):
{
  "uploaded_chunks": 5,
  "total_chunks": 10,
  "bytes_uploaded": 524288000,
  "total_bytes": 1073741824,
  "percent": 50.0,
  "status": "UPLOADING"
}
```

**Usage:**
```redis
# Worker publishes after each chunk
PUBLISH upload:progress:abc123 '{"uploaded_chunks":5,"total_chunks":10,"percent":50.0}'

# Client subscribes for real-time updates
SUBSCRIBE upload:progress:abc123
```

## Workflow Patterns

### Pattern 1: Chunk Ingestion (Service)

```python
def upload_chunk(request):
    upload_id = request.upload_id
    chunk_number = request.chunk_number
    data = request.data
    is_last = request.is_last

    # Store chunk data with 1-hour TTL
    redis.setex(
        f'upload:{upload_id}:chunks:{chunk_number}',
        3600,
        data
    )

    # Update state
    redis.hincrby(f'upload:{upload_id}:state', 'received_chunks', 1)
    redis.hset(f'upload:{upload_id}:state', 'updated_at', now_millis())

    if is_last:
        redis.hset(f'upload:{upload_id}:state', 'is_last_received', 'true')
        total_chunks = chunk_number + 1
        redis.hset(f'upload:{upload_id}:state', 'total_chunks', total_chunks)

    # Notify workers
    redis.publish('upload:chunk:received', json.dumps({
        'upload_id': upload_id,
        'chunk_number': chunk_number,
        'is_last': is_last
    }))

    # Return ACK immediately
    return UploadChunkResponse(
        node_id=request.node_id,
        state=UploadState.UPLOADING,
        chunk_number=chunk_number
    )
```

### Pattern 2: Chunk Processing (Worker)

```python
def process_chunk(upload_id, chunk_number):
    # Atomically claim chunk (GETDEL removes it so other workers can't claim)
    chunk_data = redis.getdel(f'upload:{upload_id}:chunks:{chunk_number}')

    if chunk_data is None:
        # Already processed by another worker
        return

    # Get upload state
    state = redis.hgetall(f'upload:{upload_id}:state')
    s3_upload_id = state['s3_upload_id']
    s3_key = state['s3_key']

    # Upload to S3 as part (1-based part numbers)
    part_number = chunk_number + 1
    etag = s3_client.upload_part(
        Bucket=bucket,
        Key=s3_key,
        UploadId=s3_upload_id,
        PartNumber=part_number,
        Body=chunk_data
    )['ETag']

    # Store ETag in sorted set
    redis.zadd(f'upload:{upload_id}:etags', {etag: chunk_number})

    # Update progress
    uploaded_chunks = redis.hincrby(f'upload:{upload_id}:state', 'uploaded_chunks', 1)
    redis.hset(f'upload:{upload_id}:state', 'updated_at', now_millis())

    # Publish progress
    state = redis.hgetall(f'upload:{upload_id}:state')
    redis.publish(f'upload:progress:{upload_id}', json.dumps({
        'uploaded_chunks': int(state['uploaded_chunks']),
        'total_chunks': int(state.get('total_chunks', 0)),
        'status': state['status']
    }))

    # Check if upload complete
    check_and_complete_upload(upload_id, state)
```

### Pattern 3: Upload Completion (Worker)

```python
def check_and_complete_upload(upload_id, state):
    # Only proceed if we have all chunks
    if not state.get('is_last_received'):
        return  # Don't know total chunks yet

    total_chunks = int(state['total_chunks'])
    uploaded_chunks = int(state['uploaded_chunks'])

    if uploaded_chunks < total_chunks:
        return  # Still waiting for chunks

    # Check we have all ETags
    etag_count = redis.zcard(f'upload:{upload_id}:etags')
    if etag_count != total_chunks:
        # This shouldn't happen, but check anyway
        logger.warning(f'Upload {upload_id}: chunk count mismatch')
        return

    # Mark as completing (prevents race conditions)
    if not redis.hsetnx(f'upload:{upload_id}:state', 'status', 'COMPLETING'):
        # Another worker is already completing
        return

    # Get ordered ETags
    etags_ordered = redis.zrange(f'upload:{upload_id}:etags', 0, -1)

    # Build S3 multipart completion structure
    parts = [
        {'PartNumber': i + 1, 'ETag': etag}
        for i, etag in enumerate(etags_ordered)
    ]

    # Complete S3 multipart upload
    result = s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=state['s3_key'],
        UploadId=state['s3_upload_id'],
        MultipartUpload={'Parts': parts}
    )

    final_etag = result['ETag']

    # Update state
    redis.hset(f'upload:{upload_id}:state', 'status', 'COMPLETED')
    redis.hset(f'upload:{upload_id}:state', 'final_etag', final_etag)
    redis.hset(f'upload:{upload_id}:state', 'updated_at', now_millis())

    # Update database
    db.execute(
        'UPDATE nodes SET status = ?, s3_etag = ?, size = ? WHERE node_id = ?',
        ('ACTIVE', final_etag, state['expected_size'], state['node_id'])
    )

    # Publish Kafka event
    kafka_producer.send('repository.uploads.completed', {
        'node_id': state['node_id'],
        's3_key': state['s3_key'],
        'etag': final_etag,
        'size': state['expected_size'],
        'timestamp': now_millis()
    })

    # Publish final progress
    redis.publish(f'upload:progress:{upload_id}', json.dumps({
        'status': 'COMPLETED',
        'uploaded_chunks': total_chunks,
        'total_chunks': total_chunks,
        'percent': 100.0
    }))

    # Schedule cleanup (keep state for 1 hour for status queries)
    schedule_cleanup(upload_id, delay=3600)
```

## Memory Management

### Chunk Storage Limits

Redis memory usage per upload:

```
Chunk data: N chunks × avg_chunk_size
  Example: 10 chunks × 100MB = 1GB (!)

State hash: ~1KB
ETags sorted set: N × ~50 bytes
  Example: 10 parts × 50 = 500 bytes

Total: ~1GB + 1.5KB ≈ 1GB
```

**Problem:** Large uploads consume significant Redis memory.

**Solutions:**

1. **Small chunk TTL** (1 hour)
   - Chunks auto-expire if not processed
   - Failed uploads clean up automatically

2. **Aggressive cleanup** after S3 upload
   - Worker DELs chunk immediately after uploading to S3
   - Typical lifetime: seconds to minutes, not hours

3. **Redis eviction policy**
   ```redis
   maxmemory-policy allkeys-lru
   ```
   - Evict least recently used keys when memory full
   - Chunk data evicted before state/etags (accessed more frequently)

4. **Separate Redis instance** for chunks
   - Dedicate one Redis for chunk storage (high memory, fast eviction)
   - Separate Redis for state/coordination (low memory, persistent)

### TTL Strategy

| Key Pattern | TTL | Rationale |
|-------------|-----|-----------|
| `upload:{id}:chunks:{n}` | 1 hour | Temporary, deleted after S3 upload |
| `upload:{id}:state` | 24 hours | Support status queries after completion |
| `upload:{id}:etags` | 24 hours | May need to retry completion |
| `upload:{id}:metadata` | 24 hours | Support re-indexing |

### Cleanup Process

```python
def cleanup_upload(upload_id):
    # Delete all chunk keys (if any remain)
    chunk_keys = redis.keys(f'upload:{upload_id}:chunks:*')
    if chunk_keys:
        redis.delete(*chunk_keys)

    # Delete state keys after delay (1 hour after completion)
    redis.delete(
        f'upload:{upload_id}:state',
        f'upload:{upload_id}:etags',
        f'upload:{upload_id}:metadata'
    )
```

## Failure Scenarios

### Scenario 1: Worker Crashes Mid-Processing

**Detection:**
- Chunk data exists in Redis
- No ETag in sorted set
- No progress for N minutes

**Recovery:**
- Another worker picks up chunk from pub/sub or scheduled retry
- Idempotent: S3 UploadPart with same part number overwrites

### Scenario 2: S3 Upload Fails

**Detection:**
- S3 returns error during UploadPart

**Recovery:**
- Worker retries with exponential backoff
- If all retries fail: mark upload FAILED
- Chunk data still in Redis (until TTL expires)
- Can manually retry or client restarts upload

### Scenario 3: Redis Evicts Chunk Data

**Detection:**
- Worker tries GET, receives nil

**Recovery:**
- Mark upload FAILED
- Client must restart upload
- Prevent by: dedicated Redis, appropriate memory limits

### Scenario 4: Duplicate Chunk Submission

**Detection:**
- Client sends same chunk_number twice

**Prevention:**
```python
# Service checks if chunk already uploaded
existing = redis.exists(f'upload:{upload_id}:chunks:{chunk_number}')
if existing:
    # Idempotent: return success, don't re-queue
    return UploadChunkResponse(success=True)
```

## Performance Tuning

### Redis Configuration

```conf
# Memory
maxmemory 16gb
maxmemory-policy allkeys-lru

# Persistence (for state, not chunks)
save 900 1        # Snapshot every 15 min if 1 key changed
save 300 10       # Snapshot every 5 min if 10 keys changed
save 60 10000     # Snapshot every 1 min if 10K keys changed

# Pub/Sub
client-output-buffer-limit pubsub 32mb 8mb 60

# Performance
tcp-backlog 511
timeout 300
```

### Worker Pool Sizing

```
Workers per instance: 10-50 (depends on S3 bandwidth)
  - Too few: chunks queue up in Redis
  - Too many: S3 rate limits, diminishing returns

Recommended: Start with 20, monitor queue depth
```

### Monitoring Metrics

```python
# Track in Prometheus/Grafana
chunks_queued = redis.dbsize()  # Approximate chunks in queue
upload_latency = histogram('upload_chunk_latency')  # Client → ACK time
s3_upload_latency = histogram('s3_upload_latency')  # Redis → S3 time
completion_latency = histogram('upload_completion_latency')  # First chunk → completed
```

## Next: S3 Multipart Upload

See [03-s3-multipart.md](03-s3-multipart.md) for detailed S3 multipart upload coordination.
