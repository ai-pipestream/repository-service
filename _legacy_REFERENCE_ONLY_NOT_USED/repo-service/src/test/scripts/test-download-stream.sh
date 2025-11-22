#!/bin/bash
# Test download and streaming operations for repository service

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "Testing Download/Streaming Operations"
echo "========================================"

# Test 1: Create a large document for streaming test
echo -e "\n${YELLOW}Test 1: Creating a large document for streaming${NC}"

# Generate a large payload (simulating a 10MB file)
LARGE_CONTENT=$(dd if=/dev/urandom bs=1024 count=10240 2>/dev/null | base64 -w 0)
DOCUMENT_ID="large-doc-$(date +%s)"

# For large files, we should use the upload service instead
echo "Initiating multipart upload for large file..."

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "name": "large-test-file.bin",
    "expected_size": 10485760,
    "mime_type": "application/octet-stream",
    "connector_id": "test-connector",
    "client_node_id": "'$DOCUMENT_ID'"
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.upload.NodeUploadService/InitiateUpload

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Large document upload initiated${NC}"
else
  echo -e "${RED}✗ Large document upload initiation failed${NC}"
fi

# Test 2: Download with payload (full content)
echo -e "\n${YELLOW}Test 2: Downloading document with full payload${NC}"

# First create a smaller test document
SMALL_DOC_ID="download-test-$(date +%s)"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$SMALL_DOC_ID'",
    "connector_id": "test-connector",
    "name": "download-test.txt",
    "content_type": "text/plain",
    "payload": {
      "@type": "type.googleapis.com/google.protobuf.StringValue",
      "value": "This is the content to download"
    }
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/CreateNode > /dev/null 2>&1

# Now download it with payload
echo "Fetching document with payload..."
grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$SMALL_DOC_ID'",
    "include_payload": true
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/GetNode | jq -r '.payload.value' 2>/dev/null

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Document downloaded with payload${NC}"
else
  echo -e "${RED}✗ Document download failed${NC}"
fi

# Test 3: Metadata-only fetch (for indexing)
echo -e "\n${YELLOW}Test 3: Fetching metadata only (search service pattern)${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$SMALL_DOC_ID'",
    "include_payload": false
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/GetNode | jq '{documentId: .documentId, name: .name, size: .sizeBytes, contentType: .contentType}'

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Metadata fetched successfully${NC}"
else
  echo -e "${RED}✗ Metadata fetch failed${NC}"
fi

# Test 4: Batch metadata fetch for multiple documents
echo -e "\n${YELLOW}Test 4: Batch metadata fetch (search results pattern)${NC}"

# Create a few test documents
for i in {1..3}; do
  grpcurl -plaintext \
    -d '{
      "drive": "'$DRIVE_NAME'",
      "document_id": "batch-test-'$i'-'$(date +%s)'",
      "connector_id": "test-connector",
      "name": "batch-test-'$i'.txt",
      "content_type": "text/plain",
      "payload": {
        "@type": "type.googleapis.com/google.protobuf.StringValue",
        "value": "Content for document '$i'"
      }
    }' \
    $GRPC_HOST \
    io.pipeline.repository.filesystem.FilesystemService/CreateNode > /dev/null 2>&1
done

# Search for them without payloads
echo "Searching for batch documents (metadata only)..."
grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "query": "batch-test",
    "page_size": 10
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/SearchNodes | jq '.nodes[] | {documentId: .node.documentId, name: .node.name, score: .score}'

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Batch metadata search successful${NC}"
else
  echo -e "${RED}✗ Batch metadata search failed${NC}"
fi

# Test 5: Direct S3 URL generation (future feature)
echo -e "\n${YELLOW}Test 5: Pre-signed S3 URL (future feature)${NC}"
echo -e "${YELLOW}Note: This would allow direct browser downloads${NC}"
echo "Would call: GetDocumentUrl(documentId) -> pre-signed S3 URL"
echo "Clients could then download directly from S3 without going through gRPC"

# Cleanup
echo -e "\n${YELLOW}Cleaning up test documents${NC}"

# Delete the test documents
grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$SMALL_DOC_ID'",
    "recursive": false
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/DeleteNode > /dev/null 2>&1

echo -e "${GREEN}✓ Cleanup complete${NC}"

echo -e "\n========================================"
echo "Download/Streaming Test Complete"
echo "========================================"
echo ""
echo "Summary:"
echo "- GetNode with include_payload=false: Metadata only (fast, for search/indexing)"
echo "- GetNode with include_payload=true: Full content (for actual processing)"
echo "- SearchNodes: Always metadata only (efficient for large result sets)"
echo "- Future: Pre-signed S3 URLs for direct downloads"