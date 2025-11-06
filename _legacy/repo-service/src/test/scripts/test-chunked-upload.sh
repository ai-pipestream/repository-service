#!/bin/bash
# Test script for verifying chunked upload functionality with S3/MinIO

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

echo "=== Testing Chunked Upload with LARGE Chunks (5MB+) ==="
echo

# Step 1: Initiate upload for 10MB file
echo "1. Initiating upload for 10MB file..."
RESPONSE=$(grpcurl -plaintext \
  -d '{
    "drive": "test-drive",
    "name": "large-test-file.bin",
    "expected_size": 10485760,
    "mime_type": "application/octet-stream",
    "connector_id": "test-connector-001"
  }' \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

NODE_ID=$(echo "$RESPONSE" | grep -o '"node_id": "[^"]*"' | cut -d'"' -f4)
UPLOAD_ID=$(echo "$RESPONSE" | grep -o '"upload_id": "[^"]*"' | cut -d'"' -f4)

echo "✅ Node ID: $NODE_ID"
echo "✅ Upload ID: $UPLOAD_ID"
echo

# Step 2: Create test data chunks (2.5MB each, 4 chunks = 10MB total)
echo "2. Creating 2.5MB chunks..."
CHUNK1=$(dd if=/dev/urandom bs=1024 count=2560 2>/dev/null | base64 -w0)
CHUNK2=$(dd if=/dev/urandom bs=1024 count=2560 2>/dev/null | base64 -w0)
CHUNK3=$(dd if=/dev/urandom bs=1024 count=2560 2>/dev/null | base64 -w0)
CHUNK4=$(dd if=/dev/urandom bs=1024 count=2560 2>/dev/null | base64 -w0)

echo "✅ Created 4 chunks of 2.5MB each (10MB total)"
echo

# Step 3: Upload chunks using streaming
echo "3. Uploading chunks (streaming)..."
# Upload first chunk
echo "Uploading chunk 1/4..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 1,
  \"data\": \"$CHUNK1\",
  \"is_last\": false
}" | grpcurl -plaintext \
  -d @ \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/UploadChunks

# Upload second chunk
echo "Uploading chunk 2/4..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 2,
  \"data\": \"$CHUNK2\",
  \"is_last\": false
}" | grpcurl -plaintext \
  -d @ \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/UploadChunks

# Upload third chunk
echo "Uploading chunk 3/4..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 3,
  \"data\": \"$CHUNK3\",
  \"is_last\": false
}" | grpcurl -plaintext \
  -d @ \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/UploadChunks

# Upload fourth chunk
echo "Uploading chunk 4/4..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 4,
  \"data\": \"$CHUNK4\",
  \"is_last\": true
}" | grpcurl -plaintext \
  -d @ \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/UploadChunks

echo

# Step 4: Check status
echo "4. Checking upload status..."
if [ -n "$NODE_ID" ]; then
  grpcurl -plaintext \
    -d "{\"node_id\": \"$NODE_ID\"}" \
    $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/GetUploadStatus
else
  echo "No node ID available - initiation may have failed"
fi

echo

# Step 5: Verify in MinIO
echo "5. Checking MinIO for file..."
if [ -n "$NODE_ID" ]; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://localhost:9000 s3 ls s3://modules-drive/ --recursive 2>&1 | grep "$NODE_ID" || echo "Not found"
else
  echo "No node ID to check in MinIO"
fi

echo

echo "6. Getting S3 object metadata..."
if [ -n "$NODE_ID" ]; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://localhost:9000 s3api head-object --bucket modules-drive --key "connectors/test-connector-001/$NODE_ID.pb" 2>&1 | grep -E "ContentLength|ETag|Metadata" -A5
else
  echo "No node ID to check metadata"
fi

echo
echo "=== Test Complete ==="
echo "Node ID: $NODE_ID"
echo "Expected behavior:"
echo "  - File > 5MB uses multipart upload"
echo "  - 4 chunks of 2.5MB each = 10MB total"
echo "  - With MEDIUM (10MB) chunk config: should result in 1 part"
echo "  - ETag suffix indicates number of S3 parts"
echo "  - Metadata should show upload-type: multipart"