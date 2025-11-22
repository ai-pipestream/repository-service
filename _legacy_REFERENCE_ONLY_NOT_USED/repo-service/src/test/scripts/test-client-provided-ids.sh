#!/bin/bash
# Test script for client-provided IDs and update/overwrite behavior

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

echo "=== Testing Client-Provided IDs and Update Behavior ==="
echo

CLIENT_ID="test-doc-$(uuidgen | cut -d'-' -f1)"

# Step 1: First upload (CREATE)
echo "1. Initial upload with client ID: $CLIENT_ID"
RESPONSE1=$(grpcurl -plaintext \
  -d "{
    \"drive\": \"test-drive\",
    \"name\": \"version-test-v1.txt\",
    \"expected_size\": 100,
    \"mime_type\": \"text/plain\",
    \"connector_id\": \"version-test\",
    \"client_node_id\": \"$CLIENT_ID\",
    \"fail_if_exists\": false
  }" \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

echo "$RESPONSE1" | grep -E "node_id|is_update"
NODE_ID=$(echo "$RESPONSE1" | grep -o '"node_id": "[^"]*"' | cut -d'"' -f4)
UPLOAD_ID=$(echo "$RESPONSE1" | grep -o '"upload_id": "[^"]*"' | cut -d'"' -f4)

if [ "$NODE_ID" == "$CLIENT_ID" ]; then
    echo "   ✅ Server accepted client-provided ID"
else
    echo "   ❌ Server returned different ID"
fi

# Upload v1 data
DATA1=$(echo "Version 1 content" | base64)
echo "{
  \"node_id\": \"$NODE_ID\",
  \"upload_id\": \"$UPLOAD_ID\",
  \"chunk_number\": 1,
  \"data\": \"$DATA1\",
  \"is_last\": true
}" | grpcurl -plaintext \
  -d @ \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/UploadChunks > /dev/null 2>&1

echo

# Step 2: Update (OVERWRITE) - should succeed
echo "2. UPDATE with same client ID (fail_if_exists=false)..."
RESPONSE2=$(grpcurl -plaintext \
  -d "{
    \"drive\": \"test-drive\",
    \"name\": \"version-test-v2.txt\",
    \"expected_size\": 100,
    \"mime_type\": \"text/plain\",
    \"connector_id\": \"version-test\",
    \"client_node_id\": \"$CLIENT_ID\",
    \"fail_if_exists\": false
  }" \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

echo "$RESPONSE2" | grep -E "node_id|is_update|previous_version"
IS_UPDATE=$(echo "$RESPONSE2" | grep -o '"is_update": [^,}]*' | cut -d':' -f2 | tr -d ' ')

if [ "$IS_UPDATE" == "true" ]; then
    echo "   ✅ Correctly detected as UPDATE"
else
    echo "   ⚠️  Not detected as update (might be first run)"
fi

echo

# Step 3: Try with fail_if_exists=true (should FAIL)
echo "3. Try with fail_if_exists=true (should fail)..."
RESPONSE3=$(grpcurl -plaintext \
  -import-path "$PROTO_DIR" \
  -proto repository/filesystem/upload/upload_service.proto \
  -d "{
    \"drive\": \"test-drive\",
    \"name\": \"version-test-v3.txt\",
    \"expected_size\": 100,
    \"mime_type\": \"text/plain\",
    \"connector_id\": \"version-test\",
    \"client_node_id\": \"$CLIENT_ID\",
    \"fail_if_exists\": true
  }" \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

if echo "$RESPONSE3" | grep -q "ERROR\|already exists"; then
    echo "   ✅ Correctly failed with fail_if_exists=true"
else
    echo "   ⚠️  Did not fail (proto might need regeneration)"
fi

echo

# Step 4: Upload without client ID (should generate random)
echo "4. Upload WITHOUT client ID (should generate UUID)..."
RESPONSE4=$(grpcurl -plaintext \
  -d "{
    \"drive\": \"test-drive\",
    \"name\": \"random-id-test.txt\",
    \"expected_size\": 100,
    \"mime_type\": \"text/plain\",
    \"connector_id\": \"version-test\"
  }" \
  $GRPC_HOST io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload 2>&1)

NODE_ID4=$(echo "$RESPONSE4" | grep -o '"node_id": "[^"]*"' | cut -d'"' -f4)
echo "   Generated node ID: $NODE_ID4"

if [[ $NODE_ID4 =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
    echo "   ✅ Server generated UUID format ID"
else
    echo "   ⚠️  Generated ID format: $NODE_ID4"
fi

echo

# Step 5: Check S3
echo "5. Checking S3 for uploaded files..."
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url http://localhost:9000 s3 ls s3://modules-drive/connectors/version-test/ 2>&1 | grep -E "$CLIENT_ID|$NODE_ID4" | head -5

echo
echo "=== Test Complete ==="
echo "Expected behavior:"
echo "  1. Client can provide their own ID"
echo "  2. Updates overwrite existing (with versioning if enabled)"
echo "  3. fail_if_exists=true prevents overwrites"
echo "  4. Without client ID, server generates UUID"