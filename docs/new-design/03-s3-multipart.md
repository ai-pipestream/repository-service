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

```python
if request.payload and len(request.payload) < MAX_SINGLE_PUT:
    # Direct PUT (not multipart)
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=request.payload,
        ContentType=mime_type
    )
    # Skip multipart upload entirely
    return InitiateUploadResponse(
        node_id=node_id,
        state=UploadState.COMPLETED
    )
```

**Threshold:** 100MB (configurable)
- Below threshold: Simple PUT
- Above threshold: Multipart upload

### Multipart Upload Workflow

#### Phase 1: Initiate Upload

```python
def initiate_upload(request):
    # Generate IDs
    node_id = generate_uuid()
    s3_key = f"connectors/{request.connector_id}/{node_id}.pb"

    # Initiate S3 multipart upload
    response = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=s3_key,
        ContentType=request.mime_type,
        Metadata={
            'node-id': node_id,
            'connector-id': request.connector_id,
            'original-name': request.name
        }
    )

    s3_upload_id = response['UploadId']

    # Store state in Redis
    redis.hset(f'upload:{node_id}:state',
        mapping={
            'node_id': node_id,
            's3_upload_id': s3_upload_id,
            's3_key': s3_key,
            'status': 'PENDING',
            'expected_size': request.expected_size,
            'created_at': now_millis()
        }
    )
    redis.expire(f'upload:{node_id}:state', 86400)

    # Initialize empty sorted set for ETags
    redis.zadd(f'upload:{node_id}:etags', {})
    redis.expire(f'upload:{node_id}:etags', 86400)

    return InitiateUploadResponse(
        node_id=node_id,
        upload_id=node_id,  # Use node_id as upload_id for simplicity
        state=UploadState.PENDING
    )
```

#### Phase 2: Upload Parts

```python
def process_chunk_to_s3(upload_id, chunk_number, chunk_data):
    # Get upload metadata
    state = redis.hgetall(f'upload:{upload_id}:state')
    s3_upload_id = state['s3_upload_id']
    s3_key = state['s3_key']

    # S3 part numbers are 1-based
    part_number = chunk_number + 1

    # Upload part to S3
    try:
        response = s3_client.upload_part(
            Bucket=bucket,
            Key=s3_key,
            UploadId=s3_upload_id,
            PartNumber=part_number,
            Body=chunk_data,
            ContentLength=len(chunk_data)
        )

        etag = response['ETag']  # Format: "abc123..." or "abc123...-2" for multipart ETags

        # Store ETag in Redis sorted set (score = chunk_number for ordering)
        redis.zadd(f'upload:{upload_id}:etags', {etag: chunk_number})

        # Update progress
        redis.hincrby(f'upload:{upload_id}:state', 'uploaded_chunks', 1)

        logger.info(f'Uploaded part {part_number} for {upload_id}, ETag: {etag}')

        return etag

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == 'NoSuchUpload':
            # Upload was aborted or expired
            redis.hset(f'upload:{upload_id}:state', 'status', 'FAILED')
            redis.hset(f'upload:{upload_id}:state', 'error_message', 'S3 upload expired or aborted')
            raise

        elif error_code in ['RequestTimeout', 'ServiceUnavailable']:
            # Retry with exponential backoff
            retry_with_backoff(upload_id, chunk_number, chunk_data)

        else:
            # Permanent error
            redis.hset(f'upload:{upload_id}:state', 'status', 'FAILED')
            redis.hset(f'upload:{upload_id}:state', 'error_message', str(e))
            raise
```

#### Phase 3: Complete Upload

```python
def complete_multipart_upload(upload_id):
    state = redis.hgetall(f'upload:{upload_id}:state')

    # Ensure all chunks uploaded
    total_chunks = int(state.get('total_chunks', 0))
    uploaded_chunks = int(state.get('uploaded_chunks', 0))

    if uploaded_chunks < total_chunks:
        logger.warning(f'Upload {upload_id} not complete: {uploaded_chunks}/{total_chunks}')
        return False

    # Get ordered ETags from Redis sorted set
    etags_with_scores = redis.zrange(f'upload:{upload_id}:etags', 0, -1, withscores=True)

    # Build parts list for S3
    parts = []
    for etag, chunk_number in etags_with_scores:
        parts.append({
            'PartNumber': int(chunk_number) + 1,  # S3 uses 1-based part numbers
            'ETag': etag.decode() if isinstance(etag, bytes) else etag
        })

    # Verify we have sequential part numbers
    expected_parts = list(range(1, total_chunks + 1))
    actual_parts = sorted([p['PartNumber'] for p in parts])

    if actual_parts != expected_parts:
        missing = set(expected_parts) - set(actual_parts)
        logger.error(f'Upload {upload_id} missing parts: {missing}')
        redis.hset(f'upload:{upload_id}:state', 'status', 'FAILED')
        redis.hset(f'upload:{upload_id}:state', 'error_message', f'Missing parts: {missing}')
        return False

    # Complete S3 multipart upload
    try:
        response = s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=state['s3_key'],
            UploadId=state['s3_upload_id'],
            MultipartUpload={'Parts': parts}
        )

        final_etag = response['ETag']
        location = response['Location']

        # Update Redis state
        redis.hset(f'upload:{upload_id}:state', 'status', 'COMPLETED')
        redis.hset(f'upload:{upload_id}:state', 'final_etag', final_etag)
        redis.hset(f'upload:{upload_id}:state', 'completed_at', now_millis())

        # Update database
        db.execute('''
            UPDATE nodes
            SET status = 'ACTIVE',
                s3_etag = ?,
                size = ?,
                updated_at = NOW()
            WHERE node_id = ?
        ''', (final_etag, state['expected_size'], state['node_id']))

        # Publish Kafka completion event
        publish_upload_completed_event(state, final_etag)

        logger.info(f'Completed upload {upload_id}, ETag: {final_etag}')

        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f'Failed to complete upload {upload_id}: {error_code}')

        redis.hset(f'upload:{upload_id}:state', 'status', 'FAILED')
        redis.hset(f'upload:{upload_id}:state', 'error_message', str(e))

        return False
```

## Upload Cancellation

### Client-Initiated Cancellation

```python
def cancel_upload(node_id):
    state = redis.hgetall(f'upload:{node_id}:state')

    if not state:
        return {'success': False, 'message': 'Upload not found'}

    if state['status'] == 'COMPLETED':
        return {'success': False, 'message': 'Upload already completed'}

    s3_upload_id = state.get('s3_upload_id')
    s3_key = state.get('s3_key')

    if s3_upload_id:
        # Abort S3 multipart upload
        try:
            s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                UploadId=s3_upload_id
            )
        except ClientError as e:
            logger.warning(f'Failed to abort S3 upload: {e}')

    # Clean up Redis
    redis.delete(f'upload:{node_id}:state')
    redis.delete(f'upload:{node_id}:etags')
    redis.delete(f'upload:{node_id}:metadata')

    # Delete chunk data
    chunk_keys = redis.keys(f'upload:{node_id}:chunks:*')
    if chunk_keys:
        redis.delete(*chunk_keys)

    # Update database
    db.execute('UPDATE nodes SET status = ? WHERE node_id = ?', ('CANCELLED', node_id))

    return {'success': True, 'message': 'Upload cancelled'}
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

```python
def retry_with_backoff(upload_id, chunk_number, chunk_data, attempt=1, max_attempts=3):
    if attempt > max_attempts:
        logger.error(f'Max retries exceeded for upload {upload_id} chunk {chunk_number}')
        redis.hset(f'upload:{upload_id}:state', 'status', 'FAILED')
        redis.hset(f'upload:{upload_id}:state', 'error_message', 'S3 upload failed after retries')
        return None

    delay = min(2 ** attempt, 60)  # Exponential backoff, max 60 seconds
    logger.info(f'Retrying upload {upload_id} chunk {chunk_number} in {delay}s (attempt {attempt})')
    time.sleep(delay)

    try:
        return process_chunk_to_s3(upload_id, chunk_number, chunk_data)
    except Exception as e:
        logger.warning(f'Retry attempt {attempt} failed: {e}')
        return retry_with_backoff(upload_id, chunk_number, chunk_data, attempt + 1, max_attempts)
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
