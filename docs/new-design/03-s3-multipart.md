# S3 Multipart Upload Coordination

## Overview

S3 multipart uploads enable uploading large objects in parts, with benefits:
- **Parallel uploads**: Upload parts concurrently
- **Resilience**: Retry individual parts instead of entire file
- **Large files**: Support files up to 5TB
- **Resumability**: Resume failed uploads (with our Redis state)

## S3 Multipart Upload Basics

### Three-Phase Process

```
1. CreateMultipartUpload
   Input: Bucket, Key, Metadata
   Output: UploadId

2. UploadPart (repeated for each part)
   Input: Bucket, Key, UploadId, PartNumber, Body
   Output: ETag
   Constraints:
     - Part size: 5MB - 5GB (except last part, which can be smaller)
     - Max parts: 10,000
     - Part numbers: 1-10000 (1-based)

3. CompleteMultipartUpload
   Input: Bucket, Key, UploadId, Parts (ordered list of {PartNumber, ETag})
   Output: FinalETag, Location
```

### Key Constraints

- **Minimum part size**: 5MB (except last part)
- **Maximum part size**: 5GB
- **Maximum parts**: 10,000
- **Maximum object size**: 5TB
- **Part numbering**: 1-based, must be sequential for completion
- **ETag ordering**: Must match part number order in CompleteMultipartUpload

## Our Implementation Strategy

### Chunk Size Strategy

We allow flexible chunk sizes to accommodate different network conditions:

| Chunk Size | Use Case | Parts for 1GB file |
|------------|----------|-------------------|
| 5MB (min) | Slow networks, many retries | 200 parts |
| 25MB | Balanced | 40 parts |
| 100MB | Fast networks, large files | 10 parts |
| 500MB | Very fast networks | 2 parts |

**Client decides chunk size** based on:
- Network speed
- File size
- Desired parallelism
- Memory constraints

### Single-Chunk Optimization

For small files or when client sends entire file in one chunk:

```java
@ApplicationScoped
public class UploadService {

    @Inject
    S3Client s3Client;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    @ConfigProperty(name = "s3.single.put.threshold", defaultValue = "104857600") // 100MB
    long maxSinglePutSize;

    /**
     * Handle small file uploads with optimization.
     * Files under threshold use simple PUT instead of multipart upload.
     */
    public InitiateUploadResponse initiateUpload(InitiateUploadRequest request) {
        // Check if payload is present and small enough for direct upload
        if (request.hasPayload() &&
            request.getPayload().size() < maxSinglePutSize) {

            String nodeId = UUID.randomUUID().toString();
            String s3Key = buildS3Key(request.getConnectorId(), nodeId);

            // Direct PUT to S3 (not multipart) - faster for small files
            PutObjectResponse response = s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(request.getMimeType())
                    .contentLength((long) request.getPayload().size())
                    .build(),
                RequestBody.fromBytes(request.getPayload().toByteArray())
            );

            // File is already uploaded - skip multipart process entirely
            return InitiateUploadResponse.newBuilder()
                .setNodeId(nodeId)
                .setState(UploadState.COMPLETED)  // Already done!
                .setCreatedAtEpochMs(System.currentTimeMillis())
                .build();
        }

        // File is large - continue with multipart upload process
        // (rest of multipart upload logic)
    }
}
```

**Threshold:** 100MB (configurable via `s3.single.put.threshold`)
- Below threshold: Simple PUT (faster, single API call)
- Above threshold: Multipart upload (parallel, resumable)

### Multipart Upload Workflow

#### Phase 1: Initiate Upload

```java
@ApplicationScoped
public class MultipartUploadService {

    @Inject
    S3Client s3Client;

    @Inject
    RedisClient redis;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    /**
     * Initiate a multipart upload for large files.
     * This creates the upload tracking structures in both S3 and Redis.
     */
    @Transactional
    public InitiateUploadResponse initiateUpload(InitiateUploadRequest request) {
        // Generate unique node ID (document identifier)
        String nodeId = UUID.randomUUID().toString();

        // Build S3 key following naming convention
        String s3Key = String.format("connectors/%s/%s.pb",
            request.getConnectorId(), nodeId);

        // Initiate S3 multipart upload
        // This creates a placeholder in S3 that we'll add parts to
        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(
            CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(request.getMimeType())
                // Store metadata with the S3 object for future reference
                .metadata(Map.of(
                    "node-id", nodeId,
                    "connector-id", request.getConnectorId(),
                    "original-name", request.getName()
                ))
                .build()
        );

        // S3 returns an upload ID that we'll use for all subsequent part uploads
        String s3UploadId = response.uploadId();

        // Store upload state in Redis for tracking
        Map<String, String> stateMap = Map.of(
            "node_id", nodeId,
            "s3_upload_id", s3UploadId,
            "s3_key", s3Key,
            "status", "PENDING",
            "expected_size", String.valueOf(request.getExpectedSize()),
            "created_at", String.valueOf(System.currentTimeMillis())
        );
        redis.hset(List.of("upload:" + nodeId + ":state", stateMap));
        redis.expire("upload:" + nodeId + ":state", 86400); // 24 hour TTL

        // Initialize empty sorted set for ETags
        // This will store S3 part ETags as chunks are uploaded
        redis.zadd(List.of("upload:" + nodeId + ":etags"));
        redis.expire("upload:" + nodeId + ":etags", 86400);

        // Return immediately with node_id and upload_id
        // Client can now start uploading chunks in parallel
        return InitiateUploadResponse.newBuilder()
            .setNodeId(nodeId)
            .setUploadId(nodeId)  // Use node_id as upload_id for simplicity
            .setState(UploadState.PENDING)
            .setCreatedAtEpochMs(System.currentTimeMillis())
            .build();
    }
}
```

#### Phase 2: Upload Parts

```java
@ApplicationScoped
public class ChunkUploadWorker {

    @Inject
    S3Client s3Client;

    @Inject
    RedisClient redis;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    private static final Logger logger = Logger.getLogger(ChunkUploadWorker.class);

    /**
     * Process a chunk from Redis and upload it to S3 as a multipart upload part.
     * Handles retries and error scenarios gracefully.
     */
    public String processChunkToS3(String uploadId, int chunkNumber, byte[] chunkData) {
        // Get upload metadata from Redis
        Map<String, String> state = redis.hgetall("upload:" + uploadId + ":state")
            .toCompletableFuture().join();
        String s3UploadId = state.get("s3_upload_id");
        String s3Key = state.get("s3_key");

        // S3 part numbers are 1-based (chunk numbers are 0-based)
        int partNumber = chunkNumber + 1;

        try {
            // Upload part to S3
            // Each part must be at least 5MB except the last part
            UploadPartResponse response = s3Client.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .uploadId(s3UploadId)
                    .partNumber(partNumber)
                    .contentLength((long) chunkData.length)
                    .build(),
                RequestBody.fromBytes(chunkData)
            );

            // ETag format: "abc123..." or "abc123...-N" for multipart composite ETags
            String etag = response.eTag();

            // Store ETag in Redis sorted set
            // Score = chunk_number ensures proper ordering when we complete the upload
            redis.zadd(List.of("upload:" + uploadId + ":etags", chunkNumber, etag));

            // Update progress counter
            redis.hincrby("upload:" + uploadId + ":state", "uploaded_chunks", 1);

            logger.infof("Uploaded part %d for upload %s, ETag: %s",
                partNumber, uploadId, etag);

            return etag;

        } catch (S3Exception e) {
            String errorCode = e.awsErrorDetails().errorCode();

            if ("NoSuchUpload".equals(errorCode)) {
                // Upload was aborted or expired on S3 side
                redis.hset("upload:" + uploadId + ":state", "status", "FAILED");
                redis.hset("upload:" + uploadId + ":state", "error_message",
                    "S3 upload expired or aborted");
                throw e;
            }
            else if ("RequestTimeout".equals(errorCode) ||
                     "ServiceUnavailable".equals(errorCode)) {
                // Transient error - retry with exponential backoff
                return retryWithBackoff(uploadId, chunkNumber, chunkData);
            }
            else {
                // Permanent error - mark upload as failed
                redis.hset("upload:" + uploadId + ":state", "status", "FAILED");
                redis.hset("upload:" + uploadId + ":state", "error_message",
                    e.getMessage());
                throw e;
            }
        }
    }
}
```

#### Phase 3: Complete Upload

```java
@ApplicationScoped
public class MultipartCompletionService {

    @Inject
    S3Client s3Client;

    @Inject
    RedisClient redis;

    @Inject
    EntityManager em;

    @Inject
    EventPublisher eventPublisher;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    private static final Logger logger = Logger.getLogger(MultipartCompletionService.class);

    /**
     * Complete a multipart upload by combining all parts into a single S3 object.
     * Validates that all parts are present before completing.
     */
    @Transactional
    public boolean completeMultipartUpload(String uploadId) {
        // Retrieve upload state from Redis
        Map<String, String> state = redis.hgetall("upload:" + uploadId + ":state")
            .toCompletableFuture().join();

        // Ensure all chunks have been uploaded
        int totalChunks = Integer.parseInt(state.getOrDefault("total_chunks", "0"));
        int uploadedChunks = Integer.parseInt(state.getOrDefault("uploaded_chunks", "0"));

        if (uploadedChunks < totalChunks) {
            logger.warnf("Upload %s not complete: %d/%d chunks uploaded",
                uploadId, uploadedChunks, totalChunks);
            return false;
        }

        // Get ordered ETags from Redis sorted set
        // ZRANGE returns members sorted by score (chunk_number in our case)
        List<ScoredValue<String>> etagsWithScores = redis.zrangeWithScores(
            "upload:" + uploadId + ":etags", 0, -1
        ).toCompletableFuture().join();

        // Build parts list for S3 CompleteMultipartUpload request
        List<CompletedPart> parts = new ArrayList<>();
        for (ScoredValue<String> entry : etagsWithScores) {
            int chunkNumber = (int) entry.score();
            String etag = entry.value();
            parts.add(CompletedPart.builder()
                .partNumber(chunkNumber + 1)  // S3 uses 1-based part numbers
                .eTag(etag)
                .build());
        }

        // Verify we have sequential part numbers (no gaps)
        Set<Integer> expectedParts = IntStream.rangeClosed(1, totalChunks)
            .boxed()
            .collect(Collectors.toSet());
        Set<Integer> actualParts = parts.stream()
            .map(CompletedPart::partNumber)
            .collect(Collectors.toSet());

        if (!actualParts.equals(expectedParts)) {
            Set<Integer> missing = new HashSet<>(expectedParts);
            missing.removeAll(actualParts);
            logger.errorf("Upload %s missing parts: %s", uploadId, missing);
            redis.hset("upload:" + uploadId + ":state", "status", "FAILED");
            redis.hset("upload:" + uploadId + ":state", "error_message",
                "Missing parts: " + missing);
            return false;
        }

        // Complete S3 multipart upload - combines all parts into single object
        try {
            CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(state.get("s3_key"))
                    .uploadId(state.get("s3_upload_id"))
                    .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(parts)
                        .build())
                    .build()
            );

            String finalEtag = response.eTag();
            String location = response.location();

            // Update Redis state to reflect completion
            redis.hset("upload:" + uploadId + ":state", "status", "COMPLETED");
            redis.hset("upload:" + uploadId + ":state", "final_etag", finalEtag);
            redis.hset("upload:" + uploadId + ":state", "completed_at",
                String.valueOf(System.currentTimeMillis()));

            // Update database - mark node as ACTIVE and store final metadata
            em.createQuery("""
                UPDATE Node n
                SET n.status = 'ACTIVE',
                    n.s3Etag = :etag,
                    n.sizeBytes = :size,
                    n.updatedAt = CURRENT_TIMESTAMP
                WHERE n.documentId = :nodeId
                """)
                .setParameter("etag", finalEtag)
                .setParameter("size", Long.parseLong(state.get("expected_size")))
                .setParameter("nodeId", state.get("node_id"))
                .executeUpdate();

            // Publish Kafka completion event for downstream consumers
            eventPublisher.publishUploadCompleted(state, finalEtag);

            logger.infof("Completed upload %s, ETag: %s", uploadId, finalEtag);

            return true;

        } catch (S3Exception e) {
            String errorCode = e.awsErrorDetails().errorCode();
            logger.errorf("Failed to complete upload %s: %s", uploadId, errorCode);

            redis.hset("upload:" + uploadId + ":state", "status", "FAILED");
            redis.hset("upload:" + uploadId + ":state", "error_message", e.getMessage());

            return false;
        }
    }
}
```

## Upload Cancellation

### Client-Initiated Cancellation

```java
@ApplicationScoped
public class UploadCancellationService {

    @Inject
    S3Client s3Client;

    @Inject
    RedisClient redis;

    @Inject
    EntityManager em;

    @ConfigProperty(name = "s3.bucket.name")
    String bucketName;

    private static final Logger logger = Logger.getLogger(UploadCancellationService.class);

    /**
     * Cancel an in-progress upload.
     * Aborts the S3 multipart upload and cleans up all related state.
     */
    @Transactional
    public Map<String, Object> cancelUpload(String nodeId) {
        // Get upload state from Redis
        Map<String, String> state = redis.hgetall("upload:" + nodeId + ":state")
            .toCompletableFuture().join();

        if (state.isEmpty()) {
            return Map.of(
                "success", false,
                "message", "Upload not found"
            );
        }

        if ("COMPLETED".equals(state.get("status"))) {
            return Map.of(
                "success", false,
                "message", "Upload already completed"
            );
        }

        String s3UploadId = state.get("s3_upload_id");
        String s3Key = state.get("s3_key");

        // Abort S3 multipart upload to free up S3 resources
        // S3 charges for incomplete multipart uploads, so this is important
        if (s3UploadId != null) {
            try {
                s3Client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .uploadId(s3UploadId)
                        .build()
                );
            } catch (S3Exception e) {
                logger.warnf("Failed to abort S3 upload %s: %s",
                    s3UploadId, e.getMessage());
                // Continue with cleanup even if S3 abort fails
            }
        }

        // Clean up Redis state
        redis.del("upload:" + nodeId + ":state");
        redis.del("upload:" + nodeId + ":etags");
        redis.del("upload:" + nodeId + ":metadata");

        // Delete all chunk data from Redis
        Set<String> chunkKeys = redis.keys("upload:" + nodeId + ":chunks:*")
            .toCompletableFuture().join();
        if (!chunkKeys.isEmpty()) {
            redis.del(chunkKeys.toArray(new String[0]));
        }

        // Update database - mark node as cancelled
        em.createQuery("UPDATE Node n SET n.status = 'CANCELLED' WHERE n.documentId = :nodeId")
            .setParameter("nodeId", nodeId)
            .executeUpdate();

        return Map.of(
            "success", true,
            "message", "Upload cancelled"
        );
    }
}
```

### Automatic Cleanup of Abandoned Uploads

S3 charges for incomplete multipart uploads. Clean them up regularly:

```python
# Scheduled job (daily)
def cleanup_abandoned_uploads():
    # Find uploads in PENDING/UPLOADING state older than 24 hours
    cutoff = now_millis() - (24 * 3600 * 1000)

    upload_keys = redis.keys('upload:*:state')

    for key in upload_keys:
        state = redis.hgetall(key)

        created_at = int(state.get('created_at', 0))
        status = state.get('status', '')

        if status in ['PENDING', 'UPLOADING'] and created_at < cutoff:
            upload_id = extract_upload_id(key)
            logger.info(f'Cleaning up abandoned upload: {upload_id}')
            cancel_upload(upload_id)

    # Also use S3 lifecycle policies
    # Configure bucket lifecycle rule to auto-delete incomplete uploads after 7 days
```

**S3 Lifecycle Policy:**
```xml
<LifecycleConfiguration>
    <Rule>
        <ID>DeleteIncompleteMultipartUploads</ID>
        <Status>Enabled</Status>
        <AbortIncompleteMultipartUpload>
            <DaysAfterInitiation>7</DaysAfterInitiation>
        </AbortIncompleteMultipartUpload>
    </Rule>
</LifecycleConfiguration>
```

## Error Handling

### Retry Logic

```java
@ApplicationScoped
public class RetryService {

    @Inject
    ChunkUploadWorker uploadWorker;

    @Inject
    RedisClient redis;

    private static final Logger logger = Logger.getLogger(RetryService.class);
    private static final int MAX_ATTEMPTS = 3;

    /**
     * Retry chunk upload with exponential backoff.
     * Handles transient S3 failures gracefully.
     *
     * @param uploadId The upload ID
     * @param chunkNumber The chunk number to retry
     * @param chunkData The chunk data bytes
     * @param attempt Current attempt number (1-based)
     * @return The ETag if successful, null if all retries exhausted
     */
    public String retryWithBackoff(String uploadId, int chunkNumber,
                                   byte[] chunkData, int attempt) {
        if (attempt > MAX_ATTEMPTS) {
            logger.errorf("Max retries (%d) exceeded for upload %s chunk %d",
                MAX_ATTEMPTS, uploadId, chunkNumber);

            // Mark upload as failed after exhausting retries
            redis.hset("upload:" + uploadId + ":state", "status", "FAILED");
            redis.hset("upload:" + uploadId + ":state", "error_message",
                "S3 upload failed after " + MAX_ATTEMPTS + " retries");
            return null;
        }

        // Exponential backoff: 2^attempt seconds, max 60 seconds
        // Attempt 1: 2s, Attempt 2: 4s, Attempt 3: 8s
        long delaySeconds = Math.min((long) Math.pow(2, attempt), 60);

        logger.infof("Retrying upload %s chunk %d in %ds (attempt %d/%d)",
            uploadId, chunkNumber, delaySeconds, attempt, MAX_ATTEMPTS);

        try {
            // Wait before retrying
            Thread.sleep(delaySeconds * 1000);

            // Retry the upload
            return uploadWorker.processChunkToS3(uploadId, chunkNumber, chunkData);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.errorf("Retry interrupted for upload %s chunk %d", uploadId, chunkNumber);
            return null;

        } catch (Exception e) {
            logger.warnf("Retry attempt %d failed for upload %s chunk %d: %s",
                attempt, uploadId, chunkNumber, e.getMessage());

            // Recursive retry with incremented attempt counter
            return retryWithBackoff(uploadId, chunkNumber, chunkData, attempt + 1);
        }
    }

    /**
     * Convenience method to start retry from attempt 1.
     */
    public String retryWithBackoff(String uploadId, int chunkNumber, byte[] chunkData) {
        return retryWithBackoff(uploadId, chunkNumber, chunkData, 1);
    }
}
```

### Part Number Conflicts

S3 allows overwriting parts with the same part number:

```python
# If client sends duplicate chunk, we can safely re-upload
# The new ETag will replace the old one in Redis sorted set

redis.zadd(f'upload:{upload_id}:etags', {new_etag: chunk_number})
# This overwrites any existing ETag for this chunk_number
```

### Missing Parts Detection

```python
def verify_all_parts_present(upload_id, expected_count):
    etag_count = redis.zcard(f'upload:{upload_id}:etags')

    if etag_count < expected_count:
        # Get which parts we have
        parts_present = [int(score) for _, score in redis.zrange(f'upload:{upload_id}:etags', 0, -1, withscores=True)]
        parts_expected = list(range(expected_count))
        missing = set(parts_expected) - set(parts_present)

        logger.error(f'Upload {upload_id} missing parts: {sorted(missing)}')
        return False, missing

    return True, []
```

## S3-Specific Considerations

### ETag Format

S3 ETags have two formats:

1. **Simple PUT**: `"abc123..."` (MD5 of content)
2. **Multipart upload**: `"abc123...-N"` where N is number of parts

**Our handling:**
```python
# We don't parse ETags, just store and return them to S3
# S3 validates ETags during CompleteMultipartUpload
```

### S3 Versioning

If bucket has versioning enabled:

```python
# CompleteMultipartUpload returns VersionId
response = s3_client.complete_multipart_upload(...)

version_id = response.get('VersionId')  # Store in database/metadata

# Store version ID for potential rollback
redis.hset(f'upload:{upload_id}:state', 's3_version_id', version_id)
```

### S3 Server-Side Encryption

#### Option 1: S3-Managed Encryption (SSE-S3)

```python
# S3 manages encryption keys
s3_client.create_multipart_upload(
    Bucket=bucket,
    Key=s3_key,
    ServerSideEncryption='AES256',
    # All UploadPart calls inherit this setting
)
```

#### Option 2: KMS-Managed Encryption (SSE-KMS)

```python
# AWS KMS manages encryption keys
s3_client.create_multipart_upload(
    Bucket=bucket,
    Key=s3_key,
    ServerSideEncryption='aws:kms',
    SSEKMSKeyId='arn:aws:kms:region:account:key/key-id',
    # All UploadPart calls inherit this setting
)
```

#### Option 3: Client-Provided Encryption (SSE-C)

**For maximum security, clients can provide their own encryption keys.**

```python
# Client provides encryption key in InitiateUploadRequest
message InitiateUploadRequest {
    // ... existing fields ...
    EncryptionOptions encryption = 12;
}

message EncryptionOptions {
    EncryptionType type = 1;           // SSE_S3, SSE_KMS, SSE_C
    string customer_key_b64 = 2;       // Base64-encoded 256-bit key (for SSE-C)
    string customer_key_md5 = 3;       // MD5 of the key (for SSE-C validation)
    string kms_key_id = 4;             // KMS key ARN (for SSE-KMS)
}

enum EncryptionType {
    ENCRYPTION_TYPE_UNSPECIFIED = 0;
    SSE_S3 = 1;    // S3-managed keys
    SSE_KMS = 2;   // KMS-managed keys
    SSE_C = 3;     // Customer-provided keys
}
```

**Server-side handling for SSE-C:**

```python
def initiate_upload_with_client_encryption(request):
    encryption = request.encryption

    if encryption.type == EncryptionType.SSE_C:
        # Validate customer key
        if not encryption.customer_key_b64 or not encryption.customer_key_md5:
            raise ValueError('SSE-C requires customer_key_b64 and customer_key_md5')

        # Store encryption info in Redis (NOT the key itself, just metadata)
        redis.hset(f'upload:{node_id}:state',
            mapping={
                'encryption_type': 'SSE_C',
                # DO NOT store the actual key in Redis!
                # Key must be provided with each UploadPart request
            }
        )

        # Store key securely for this session (encrypted in Redis or in-memory only)
        # IMPORTANT: Never log or persist the raw key!
        store_encryption_key_securely(node_id, encryption.customer_key_b64)

        # CreateMultipartUpload with SSE-C
        response = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            SSECustomerAlgorithm='AES256',
            SSECustomerKey=base64.b64decode(encryption.customer_key_b64),
            SSECustomerKeyMD5=encryption.customer_key_md5
        )

    elif encryption.type == EncryptionType.SSE_KMS:
        # Use KMS key
        response = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            ServerSideEncryption='aws:kms',
            SSEKMSKeyId=encryption.kms_key_id
        )

    else:
        # Default to SSE-S3
        response = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            ServerSideEncryption='AES256'
        )

    return response


def upload_part_with_client_encryption(upload_id, part_number, chunk_data):
    state = redis.hgetall(f'upload:{upload_id}:state')
    encryption_type = state.get('encryption_type')

    if encryption_type == 'SSE_C':
        # Retrieve customer key from secure storage
        customer_key_b64, customer_key_md5 = get_encryption_key_securely(upload_id)

        # UploadPart with SSE-C (MUST provide same key as CreateMultipartUpload)
        response = s3_client.upload_part(
            Bucket=bucket,
            Key=state['s3_key'],
            UploadId=state['s3_upload_id'],
            PartNumber=part_number,
            Body=chunk_data,
            SSECustomerAlgorithm='AES256',
            SSECustomerKey=base64.b64decode(customer_key_b64),
            SSECustomerKeyMD5=customer_key_md5
        )

    else:
        # No SSE-C, regular upload
        response = s3_client.upload_part(
            Bucket=bucket,
            Key=state['s3_key'],
            UploadId=state['s3_upload_id'],
            PartNumber=part_number,
            Body=chunk_data
        )

    return response
```

**Secure Key Storage (Placeholder):**

```python
# PLACEHOLDER: Secure key storage implementation
# In production, use:
# - AWS Secrets Manager
# - HashiCorp Vault
# - Encrypted in-memory cache with short TTL
# - Never persist to disk or Redis in plaintext!

def store_encryption_key_securely(upload_id, customer_key_b64):
    """
    Store customer encryption key securely for the duration of the upload.

    Options:
    1. Encrypted in-memory cache (Redis with encryption-at-rest)
    2. AWS Secrets Manager (create temporary secret, delete after upload)
    3. HashiCorp Vault (transit encryption)
    4. Application memory with encryption wrapper

    CRITICAL: Never log or persist the raw key!
    """
    # Placeholder: Store in Redis with short TTL and encryption warning
    # TODO: Implement proper secure storage
    redis.setex(
        f'upload:{upload_id}:encryption_key',
        3600,  # 1 hour TTL
        f'ENCRYPTED:{customer_key_b64}'  # TODO: Actually encrypt this!
    )
    logger.warning(f'Upload {upload_id} using SSE-C - ensure Redis encryption-at-rest is enabled!')


def get_encryption_key_securely(upload_id):
    """Retrieve customer encryption key from secure storage."""
    # Placeholder: Retrieve from Redis
    # TODO: Decrypt if stored encrypted
    encrypted_value = redis.get(f'upload:{upload_id}:encryption_key')
    if not encrypted_value:
        raise ValueError(f'Encryption key not found for upload {upload_id}')

    customer_key_b64 = encrypted_value.decode().replace('ENCRYPTED:', '')  # TODO: Decrypt!

    # Calculate MD5 of the key for S3 validation
    key_bytes = base64.b64decode(customer_key_b64)
    key_md5 = base64.b64encode(hashlib.md5(key_bytes).digest()).decode()

    return customer_key_b64, key_md5


def cleanup_encryption_key(upload_id):
    """Delete encryption key after upload completes."""
    redis.delete(f'upload:{upload_id}:encryption_key')
    logger.info(f'Cleaned up encryption key for upload {upload_id}')
```

**Security Considerations:**

1. **Never log customer keys**: Redact from logs, error messages, metrics
2. **Never persist plaintext keys**: Use secure key management service
3. **Short-lived storage**: Delete keys immediately after upload completes
4. **Encrypted transport**: Keys sent over TLS/gRPC encrypted channels only
5. **Access control**: Only authorized workers can retrieve keys
6. **Audit trail**: Log who accessed encrypted uploads (without logging keys)

**Key Rotation Support:**

```python
# Allow client to rotate keys by re-encrypting existing object
def rotate_encryption_key(node_id, new_encryption_options):
    """
    Re-encrypt existing S3 object with new customer key.

    Process:
    1. Download object with old key
    2. Upload object with new key
    3. Update metadata
    """
    # Get old encryption info
    old_key = get_encryption_key_securely(node_id)

    # Download with old key
    response = s3_client.get_object(
        Bucket=bucket,
        Key=s3_key,
        SSECustomerAlgorithm='AES256',
        SSECustomerKey=old_key
    )

    # Upload with new key
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=response['Body'].read(),
        SSECustomerAlgorithm='AES256',
        SSECustomerKey=new_encryption_options.customer_key_b64,
        SSECustomerKeyMD5=new_encryption_options.customer_key_md5
    )

    # Update metadata
    db.execute('UPDATE nodes SET encryption_type = ?, updated_at = NOW() WHERE node_id = ?',
               ('SSE_C', node_id))
```

### S3 Transfer Acceleration

For global uploads, enable S3 Transfer Acceleration:

```python
# Use accelerated endpoint
s3_client = boto3.client(
    's3',
    config=Config(s3={'addressing_style': 'virtual'}),
    endpoint_url='https://bucket-name.s3-accelerate.amazonaws.com'
)

# All multipart upload operations use accelerated endpoint
```

## Performance Optimization

### Optimal Part Size

```python
def calculate_optimal_part_size(total_size):
    """
    Calculate optimal part size based on file size.

    Goals:
    - Min 5MB per part (S3 requirement, except last)
    - Max 10,000 parts (S3 limit)
    - Prefer larger parts for fewer API calls
    """
    min_part_size = 5 * 1024 * 1024  # 5MB
    max_parts = 10000

    # Start with 100MB parts
    preferred_part_size = 100 * 1024 * 1024

    # Adjust if file is very large
    if total_size / preferred_part_size > max_parts:
        # Need larger parts to stay under 10K limit
        part_size = (total_size // max_parts) + 1
    else:
        part_size = preferred_part_size

    # Ensure minimum part size
    part_size = max(part_size, min_part_size)

    return part_size
```

**Recommendation to clients:**
```json
{
  "recommended_chunk_size": 104857600,  // 100MB
  "min_chunk_size": 5242880,            // 5MB
  "max_chunk_size": 524288000,          // 500MB
  "max_chunks": 10000
}
```

### Parallel Upload Limits

```python
# AWS S3 best practices
MAX_CONCURRENT_UPLOADS = 10  # Per upload
MAX_RETRIES = 3
CONNECT_TIMEOUT = 10  # seconds
READ_TIMEOUT = 60     # seconds

# Configure boto3 client
config = Config(
    max_pool_connections=50,
    retries={'max_attempts': MAX_RETRIES, 'mode': 'adaptive'}
)

s3_client = boto3.client('s3', config=config)
```

### Monitoring

```python
# Track metrics
metrics.histogram('s3.upload_part.duration', duration_ms, tags={'bucket': bucket})
metrics.histogram('s3.upload_part.size', part_size, tags={'bucket': bucket})
metrics.increment('s3.upload_part.success', tags={'bucket': bucket})
metrics.increment('s3.upload_part.error', tags={'bucket': bucket, 'error_code': error_code})
```

## Testing

### Unit Tests

```python
def test_multipart_upload_completion():
    # Mock S3 responses
    s3_mock.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    s3_mock.upload_part.return_value = {'ETag': '"test-etag-1"'}

    # Initiate upload
    response = initiate_upload(request)
    upload_id = response.upload_id

    # Upload parts
    for i in range(3):
        process_chunk_to_s3(upload_id, i, b'test data')

    # Complete upload
    result = complete_multipart_upload(upload_id)

    assert result is True
    assert redis.hget(f'upload:{upload_id}:state', 'status') == 'COMPLETED'
```

### Integration Tests

```python
def test_real_s3_multipart_upload():
    # Use MinIO testcontainer
    with MinioContainer() as minio:
        s3_client = boto3.client('s3', endpoint_url=minio.get_connection_url())

        # Create test bucket
        s3_client.create_bucket(Bucket='test-bucket')

        # Test full upload flow
        # ...
```

## Next: Kafka Event Patterns

See [04-kafka-events.md](04-kafka-events.md) for Kafka event publishing and consumption patterns.
