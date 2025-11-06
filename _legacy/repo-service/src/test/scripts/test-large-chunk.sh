#!/bin/bash
# Test script for verifying large chunk splitting

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== Testing Large Chunk Splitting (25MB single chunk) ==="
echo

# Step 1: Initiate upload for 25MB file
echo "1. Initiating upload for 25MB file..."
RESPONSE=$(grpcurl -plaintext \
  -d '{
    "drive": "test-drive",
    "name": "large-chunk-test.bin",
    "expected_size": 26214400,
    "mime_type": "application/octet-stream",
    "connector_id": "test-large-chunk"
  }' \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

NODE_ID=$(echo "$RESPONSE" | grep -o '"node_id": "[^"]*"' | cut -d'"' -f4)
UPLOAD_ID=$(echo "$RESPONSE" | grep -o '"upload_id": "[^"]*"' | cut -d'"' -f4)

echo "✅ Node ID: $NODE_ID"
echo "✅ Upload ID: $UPLOAD_ID"
echo

# Step 2: Create single large chunk (25MB)
echo "2. Creating single 25MB chunk..."
LARGE_CHUNK=$(dd if=/dev/urandom bs=1024 count=25600 2>/dev/null | base64 -w0)

echo "✅ Created 1 chunk of 25MB"
echo

# Step 3: Upload the single large chunk
echo "3. Uploading single 25MB chunk (should be split internally)..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 1,
  \"data\": \"$LARGE_CHUNK\",
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
  aws --endpoint-url http://localhost:9000 s3 ls s3://modules-drive/connectors/test-large-chunk/ 2>&1 | grep "$NODE_ID" || echo "Not found"
else
  echo "No node ID to check in MinIO"
fi

echo

echo "6. Getting S3 object metadata..."
if [ -n "$NODE_ID" ]; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://localhost:9000 s3api head-object --bucket modules-drive --key "connectors/test-large-chunk/$NODE_ID.pb" 2>&1 | \
  grep -E "ContentLength|ETag|Metadata" -A5
else
  echo "No node ID to check metadata"
fi

echo
echo "=== Test Complete ==="
echo "Node ID: $NODE_ID"
echo "Expected behavior:"
echo "  - Single 25MB chunk sent by client"
echo "  - With MEDIUM (10MB) config: should be split into 3 S3 parts"
echo "  - Part 1: 10MB, Part 2: 10MB, Part 3: 5MB"
echo "  - ETag should have '-3' suffix indicating 3 parts"