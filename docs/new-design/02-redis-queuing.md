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
```java
// Worker subscribes to channel using Reactive Messaging
@ApplicationScoped
public class ChunkWorker {

    @Inject
    RedisClient redis;

    @Inject
    S3Client s3Client;

    @Incoming("upload-chunk-received")
    public CompletionStage<Void> processChunk(String message) {
        JsonObject data = Json.createReader(new StringReader(message)).readObject();
        String uploadId = data.getString("upload_id");
        int chunkNumber = data.getInt("chunk_number");

        // Try to claim the chunk (GETDEL is atomic)
        Response<byte[]> response = redis.getdel("upload:" + uploadId + ":chunks:" + chunkNumber)
            .toCompletableFuture().get();
        byte[] chunkData = response != null ? response.toBytes() : null;

        if (chunkData == null) {
            // Another worker claimed it already
            return CompletableFuture.completedFuture(null);
        }

        // Process chunk: upload to S3, store ETag
        String etag = uploadToS3(chunkData, uploadId, chunkNumber);
        redis.zadd(List.of("upload:" + uploadId + ":etags", chunkNumber, etag));
        redis.hincrby("upload:" + uploadId + ":state", "uploaded_chunks", 1);

        // Check if upload complete
        checkAndCompleteUpload(uploadId);

        return CompletableFuture.completedFuture(null);
    }
}
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

```java
@ApplicationScoped
public class UploadService {

    @Inject
    RedisClient redis;

    @Inject
    @Channel("upload-chunk-notifications")
    Emitter<String> chunkNotifier;

    public UploadChunkResponse uploadChunk(UploadChunkRequest request) {
        String uploadId = request.getUploadId();
        int chunkNumber = request.getChunkNumber();
        byte[] data = request.getData().toByteArray();
        boolean isLast = request.getIsLast();

        // Store chunk data with 1-hour TTL
        String chunkKey = String.format("upload:%s:chunks:%d", uploadId, chunkNumber);
        redis.setex(chunkKey, 3600, data);

        // Update state
        redis.hincrby("upload:" + uploadId + ":state", "received_chunks", 1);
        redis.hset("upload:" + uploadId + ":state", "updated_at", String.valueOf(System.currentTimeMillis()));

        if (isLast) {
            redis.hset("upload:" + uploadId + ":state", "is_last_received", "true");
            int totalChunks = chunkNumber + 1;
            redis.hset("upload:" + uploadId + ":state", "total_chunks", String.valueOf(totalChunks));
        }

        // Notify workers
        JsonObject notification = Json.createObjectBuilder()
            .add("upload_id", uploadId)
            .add("chunk_number", chunkNumber)
            .add("is_last", isLast)
            .build();
        chunkNotifier.send(notification.toString());

        // Return ACK immediately
        return UploadChunkResponse.newBuilder()
            .setNodeId(request.getNodeId())
            .setState(UploadState.UPLOADING)
            .setChunkNumber(chunkNumber)
            .build();
    }
}
```

### Pattern 2: Chunk Processing (Worker)

```java
@ApplicationScoped
public class ChunkProcessor {

    @Inject
    RedisClient redis;

    @Inject
    S3Client s3Client;

    @Inject
    @Channel("upload-progress")
    Emitter<String> progressEmitter;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    public void processChunk(String uploadId, int chunkNumber) {
        // Atomically claim chunk (GETDEL removes it so other workers can't claim)
        String chunkKey = String.format("upload:%s:chunks:%d", uploadId, chunkNumber);
        byte[] chunkData = redis.getdel(chunkKey).toCompletableFuture().join().toBytes();

        if (chunkData == null) {
            // Already processed by another worker
            return;
        }

        // Get upload state
        Map<String, String> state = redis.hgetall("upload:" + uploadId + ":state")
            .toCompletableFuture().join();
        String s3UploadId = state.get("s3_upload_id");
        String s3Key = state.get("s3_key");

        // Upload to S3 as part (1-based part numbers)
        int partNumber = chunkNumber + 1;
        UploadPartResponse response = s3Client.uploadPart(
            UploadPartRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .uploadId(s3UploadId)
                .partNumber(partNumber)
                .build(),
            RequestBody.fromBytes(chunkData)
        );
        String etag = response.eTag();

        // Store ETag in sorted set
        redis.zadd(List.of("upload:" + uploadId + ":etags", chunkNumber, etag));

        // Update progress
        long uploadedChunks = redis.hincrby("upload:" + uploadId + ":state", "uploaded_chunks", 1)
            .toCompletableFuture().join();
        redis.hset("upload:" + uploadId + ":state", "updated_at",
            String.valueOf(System.currentTimeMillis()));

        // Publish progress
        Map<String, String> updatedState = redis.hgetall("upload:" + uploadId + ":state")
            .toCompletableFuture().join();
        JsonObject progress = Json.createObjectBuilder()
            .add("uploaded_chunks", Integer.parseInt(updatedState.get("uploaded_chunks")))
            .add("total_chunks", Integer.parseInt(updatedState.getOrDefault("total_chunks", "0")))
            .add("status", updatedState.get("status"))
            .build();
        progressEmitter.send(progress.toString());

        // Check if upload complete
        checkAndCompleteUpload(uploadId, updatedState);
    }
}
```

### Pattern 3: Upload Completion (Worker)

```java
@ApplicationScoped
public class UploadCompletionService {

    @Inject
    RedisClient redis;

    @Inject
    S3Client s3Client;

    @Inject
    EntityManager em;

    @Inject
    @Channel("repository-events")
    Emitter<String> kafkaEmitter;

    @Inject
    @Channel("upload-progress")
    Emitter<String> progressEmitter;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    @Transactional
    public void checkAndCompleteUpload(String uploadId, Map<String, String> state) {
        // Only proceed if we have all chunks
        if (!"true".equals(state.get("is_last_received"))) {
            // Don't know total chunks yet - wait for more
            return;
        }

        int totalChunks = Integer.parseInt(state.get("total_chunks"));
        int uploadedChunks = Integer.parseInt(state.get("uploaded_chunks"));

        if (uploadedChunks < totalChunks) {
            // Still waiting for chunks to upload to S3
            return;
        }

        // Check we have all ETags (one for each chunk)
        long etagCount = redis.zcard("upload:" + uploadId + ":etags")
            .toCompletableFuture().join();
        if (etagCount != totalChunks) {
            // This shouldn't happen, but check anyway for data integrity
            logger.warn("Upload {}: chunk count mismatch - expected {} ETags but found {}",
                uploadId, totalChunks, etagCount);
            return;
        }

        // Mark as completing (prevents race conditions - only one worker should complete)
        // HSETNX only sets if field doesn't exist (atomic operation)
        Boolean wasSet = redis.hsetnx("upload:" + uploadId + ":state", "status", "COMPLETING")
            .toCompletableFuture().join();
        if (!wasSet) {
            // Another worker is already completing this upload
            return;
        }

        // Get ordered ETags from Redis sorted set (ordered by chunk number)
        List<String> etagsOrdered = redis.zrange("upload:" + uploadId + ":etags", 0, -1)
            .toCompletableFuture().join();

        // Build S3 multipart completion structure
        // S3 requires part numbers (1-based) with corresponding ETags
        List<CompletedPart> parts = new ArrayList<>();
        for (int i = 0; i < etagsOrdered.size(); i++) {
            parts.add(CompletedPart.builder()
                .partNumber(i + 1)  // S3 uses 1-based part numbers
                .eTag(etagsOrdered.get(i))
                .build());
        }

        // Complete S3 multipart upload - combines all parts into single object
        CompleteMultipartUploadResponse result = s3Client.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(state.get("s3_key"))
                .uploadId(state.get("s3_upload_id"))
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .build()
        );

        String finalEtag = result.eTag();

        // Update Redis state to reflect completion
        redis.hset("upload:" + uploadId + ":state", "status", "COMPLETED");
        redis.hset("upload:" + uploadId + ":state", "final_etag", finalEtag);
        redis.hset("upload:" + uploadId + ":state", "updated_at",
            String.valueOf(System.currentTimeMillis()));

        // Update database - mark node as ACTIVE (ready for use)
        em.createQuery("UPDATE Node n SET n.status = :status, n.s3Etag = :etag, " +
                      "n.sizeBytes = :size WHERE n.documentId = :nodeId")
            .setParameter("status", "ACTIVE")
            .setParameter("etag", finalEtag)
            .setParameter("size", Long.parseLong(state.get("expected_size")))
            .setParameter("nodeId", state.get("node_id"))
            .executeUpdate();

        // Publish Kafka event for downstream consumers (indexing, analytics, etc.)
        JsonObject event = Json.createObjectBuilder()
            .add("node_id", state.get("node_id"))
            .add("s3_key", state.get("s3_key"))
            .add("etag", finalEtag)
            .add("size", Long.parseLong(state.get("expected_size")))
            .add("timestamp", System.currentTimeMillis())
            .build();
        kafkaEmitter.send(event.toString());

        // Publish final progress update for clients
        JsonObject progress = Json.createObjectBuilder()
            .add("status", "COMPLETED")
            .add("uploaded_chunks", totalChunks)
            .add("total_chunks", totalChunks)
            .add("percent", 100.0)
            .build();
        progressEmitter.send(progress.toString());

        // Schedule cleanup (keep state for 1 hour for status queries, then remove)
        scheduleCleanup(uploadId, Duration.ofHours(1));
    }
}
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

```java
@ApplicationScoped
public class UploadCleanupService {

    @Inject
    RedisClient redis;

    /**
     * Clean up Redis keys associated with a completed or failed upload.
     * This frees up memory and prevents key accumulation.
     *
     * @param uploadId The upload ID to clean up
     */
    public void cleanupUpload(String uploadId) {
        // Delete all chunk keys (if any remain - should be cleaned during processing)
        // Pattern matching to find all chunk keys for this upload
        Set<String> chunkKeys = redis.keys("upload:" + uploadId + ":chunks:*")
            .toCompletableFuture().join();

        if (!chunkKeys.isEmpty()) {
            // Delete in batch for efficiency
            redis.del(chunkKeys.toArray(new String[0]));
        }

        // Delete state keys after delay (1 hour after completion)
        // These keys are kept temporarily to allow status queries
        redis.del(
            "upload:" + uploadId + ":state",
            "upload:" + uploadId + ":etags",
            "upload:" + uploadId + ":metadata"
        );
    }

    /**
     * Schedule cleanup to run after a delay.
     * Useful to keep upload state available for status queries before cleanup.
     */
    @Scheduled(every = "1h")
    public void cleanupExpiredUploads() {
        // Find all upload state keys
        Set<String> stateKeys = redis.keys("upload:*:state")
            .toCompletableFuture().join();

        long now = System.currentTimeMillis();
        long oneHourAgo = now - Duration.ofHours(1).toMillis();

        for (String key : stateKeys) {
            Map<String, String> state = redis.hgetall(key).toCompletableFuture().join();
            String status = state.get("status");
            long updatedAt = Long.parseLong(state.getOrDefault("updated_at", "0"));

            // Clean up completed/failed uploads older than 1 hour
            if (("COMPLETED".equals(status) || "FAILED".equals(status)) &&
                updatedAt < oneHourAgo) {
                String uploadId = key.replace("upload:", "").replace(":state", "");
                cleanupUpload(uploadId);
            }
        }
    }
}
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
```java
@ApplicationScoped
public class UploadService {

    @Inject
    RedisClient redis;

    /**
     * Handle chunk upload with idempotency protection.
     * If the same chunk is sent multiple times, we handle it gracefully.
     */
    public UploadChunkResponse uploadChunk(UploadChunkRequest request) {
        String uploadId = request.getUploadId();
        int chunkNumber = request.getChunkNumber();

        // Check if chunk already exists (idempotency check)
        String chunkKey = String.format("upload:%s:chunks:%d", uploadId, chunkNumber);
        Boolean exists = redis.exists(chunkKey).toCompletableFuture().join();

        if (exists) {
            // Idempotent: chunk already received, return success without re-queuing
            // This prevents duplicate processing and wasted S3 bandwidth
            return UploadChunkResponse.newBuilder()
                .setNodeId(request.getNodeId())
                .setState(UploadState.UPLOADING)
                .setChunkNumber(chunkNumber)
                .build();
        }

        // Continue with normal chunk storage...
        // (rest of upload logic)
    }
}
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

```java
@ApplicationScoped
public class UploadMetrics {

    @Inject
    MeterRegistry registry;

    @Inject
    RedisClient redis;

    // Counter for total chunks processed
    private final Counter chunksProcessedCounter;

    // Histogram for tracking upload chunk latency (client → ACK time)
    private final Timer uploadChunkLatency;

    // Histogram for tracking S3 upload latency (Redis → S3 time)
    private final Timer s3UploadLatency;

    // Histogram for tracking overall completion latency (first chunk → completed)
    private final Timer completionLatency;

    @PostConstruct
    public void init() {
        // Initialize metrics
        chunksProcessedCounter = Counter.builder("repository.chunks.processed")
            .description("Total number of chunks processed")
            .register(registry);

        uploadChunkLatency = Timer.builder("repository.upload.chunk.latency")
            .description("Time from chunk received to ACK sent")
            .register(registry);

        s3UploadLatency = Timer.builder("repository.s3.upload.latency")
            .description("Time to upload chunk from Redis to S3")
            .register(registry);

        completionLatency = Timer.builder("repository.upload.completion.latency")
            .description("Time from first chunk to upload completed")
            .register(registry);
    }

    /**
     * Track approximate number of chunks in Redis queue.
     * Called periodically to monitor queue depth.
     */
    @Scheduled(every = "30s")
    public void recordQueueDepth() {
        // Count all chunk keys in Redis (approximate queue depth)
        long chunksQueued = redis.keys("upload:*:chunks:*")
            .toCompletableFuture().join().size();

        Gauge.builder("repository.chunks.queued", () -> chunksQueued)
            .description("Approximate number of chunks waiting in Redis")
            .register(registry);
    }

    /**
     * Record chunk processing metrics.
     */
    public void recordChunkProcessed() {
        chunksProcessedCounter.increment();
    }

    /**
     * Time an operation and record the latency.
     */
    public <T> T timeOperation(Timer timer, Supplier<T> operation) {
        return timer.record(operation);
    }
}
```

## Next: S3 Multipart Upload

See [03-s3-multipart.md](03-s3-multipart.md) for detailed S3 multipart upload coordination.
