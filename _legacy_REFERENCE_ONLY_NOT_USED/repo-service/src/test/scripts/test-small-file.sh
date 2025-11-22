#!/bin/bash
# Test script for verifying small file uploads use single PUT (not multipart)

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

echo "=== Testing SMALL File Upload (< 5MB threshold) ==="
echo

# Step 1: Initiate upload for small file (1KB)
echo "1. Initiating upload for 1KB file..."
RESPONSE=$(grpcurl -plaintext \
  -d '{
    "drive": "test-drive",
    "name": "small-test-file.bin",
    "expected_size": 1024,
    "mime_type": "application/octet-stream",
    "connector_id": "test-connector-small"
  }' \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

NODE_ID=$(echo "$RESPONSE" | grep -o '"node_id": "[^"]*"' | cut -d'"' -f4)
UPLOAD_ID=$(echo "$RESPONSE" | grep -o '"upload_id": "[^"]*"' | cut -d'"' -f4)

echo "✅ Node ID: $NODE_ID"
echo "✅ Upload ID: $UPLOAD_ID"
echo

# Step 2: Create small test data (1KB)
echo "2. Creating 1KB of test data..."
CHUNK=$(dd if=/dev/urandom bs=1024 count=1 2>/dev/null | base64 -w0)

echo "✅ Created 1KB chunk"
echo

# Step 3: Upload as single chunk with isLast=true
echo "3. Uploading single chunk..."
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 1,
  \"data\": \"$CHUNK\",
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
  aws --endpoint-url http://localhost:9000 s3 ls s3://modules-drive/connectors/test-connector-small/ 2>&1 | grep "$NODE_ID"
else
  echo "No node ID to check in MinIO"
fi

echo

echo "6. Getting object metadata..."
if [ -n "$NODE_ID" ]; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://localhost:9000 s3api head-object --bucket modules-drive --key "connectors/test-connector-small/$NODE_ID.pb" 2>&1 | \
  grep -E "ContentLength|ETag|Metadata" -A5
else
  echo "No node ID to check metadata"
fi

echo
echo "=== Small File Test Complete ==="
echo "Expected behavior:"
echo "  - File < 5MB uses single PUT (not multipart)"
echo "  - ETag should NOT have '-' suffix (single part)"
echo "  - Metadata should show upload-type: single-put"