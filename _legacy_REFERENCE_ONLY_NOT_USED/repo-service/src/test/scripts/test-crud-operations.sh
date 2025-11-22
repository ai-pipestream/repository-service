#!/bin/bash
# Test CRUD operations for repository service

GRPC_HOST="localhost:38102"
DRIVE_NAME="test-drive"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "Testing CRUD Operations"
echo "========================================"

# Test 1: Create a test document
echo -e "\n${YELLOW}Test 1: Creating a test document${NC}"

# Create a simple protobuf Any payload
PAYLOAD=$(echo -n "Test document content" | base64)

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "test-doc-'$(date +%s)'",
    "connector_id": "test-connector",
    "name": "test-document.txt",
    "content_type": "text/plain",
    "payload": {
      "@type": "type.googleapis.com/google.protobuf.StringValue",
      "value": "Test document content"
    },
    "metadata": "{\"test\": \"metadata\"}",
    "include_payload": false
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/CreateNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Create document successful${NC}"
else
  echo -e "${RED}✗ Create document failed${NC}"
fi

# Test 2: Get node without payload
echo -e "\n${YELLOW}Test 2: Getting node without payload${NC}"

# First create a document and capture the generated ID
CREATE_RESPONSE=$(grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "test-doc-'$(date +%s)'",
    "connector_id": "test-connector",
    "name": "get-test.txt",
    "content_type": "text/plain",
    "payload": {
      "@type": "type.googleapis.com/google.protobuf.StringValue",
      "value": "Content for get test"
    }
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/CreateNode)

# Extract the document_id from the response
DOCUMENT_ID=$(echo "$CREATE_RESPONSE" | grep -o '"document_id": "[^"]*"' | cut -d'"' -f4)
echo "Created document with ID: $DOCUMENT_ID"

# Now get it without payload
grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$DOCUMENT_ID'",
    "include_payload": false
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/GetNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Get node without payload successful${NC}"
else
  echo -e "${RED}✗ Get node without payload failed${NC}"
fi

# Test 3: Get node with payload
echo -e "\n${YELLOW}Test 3: Getting node with payload${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$DOCUMENT_ID'",
    "include_payload": true
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/GetNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Get node with payload successful${NC}"
else
  echo -e "${RED}✗ Get node with payload failed${NC}"
fi

# Test 4: Update node metadata
echo -e "\n${YELLOW}Test 4: Updating node metadata${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$DOCUMENT_ID'",
    "name": "updated-name.txt",
    "content_type": "text/markdown",
    "metadata": "{\"updated\": true, \"version\": 2}"
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/UpdateNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Update node metadata successful${NC}"
else
  echo -e "${RED}✗ Update node metadata failed${NC}"
fi

# Test 5: Update node with new payload
echo -e "\n${YELLOW}Test 5: Updating node with new payload${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "document_id": "'$DOCUMENT_ID'",
    "payload": {
      "@type": "type.googleapis.com/google.protobuf.StringValue",
      "value": "Updated content version 2"
    }
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/UpdateNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Update node with payload successful${NC}"
else
  echo -e "${RED}✗ Update node with payload failed${NC}"
fi

# Test 6: Search nodes
echo -e "\n${YELLOW}Test 6: Searching nodes${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "query": "test",
    "page_size": 10
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/SearchNodes

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Search nodes successful${NC}"
else
  echo -e "${RED}✗ Search nodes failed${NC}"
fi

# Test 7: Get children of root
echo -e "\n${YELLOW}Test 7: Getting children of root${NC}"

grpcurl -plaintext \
  -d '{
    "drive": "'$DRIVE_NAME'",
    "parent_id": 0,
    "page_size": 10,
    "order_by": "name",
    "ascending": true
  }' \
  $GRPC_HOST \
  io.pipeline.repository.filesystem.FilesystemService/GetChildren

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Get children successful${NC}"
else
  echo -e "${RED}✗ Get children failed${NC}"
fi

# Test 8: Delete node
echo -e "\n${YELLOW}Test 8: Deleting node${NC}"

if [ -z "$DOCUMENT_ID" ]; then
  echo -e "${RED}✗ No document ID available for deletion${NC}"
else
  grpcurl -plaintext \
    -d '{
      "drive": "'$DRIVE_NAME'",
      "document_id": "'$DOCUMENT_ID'",
      "recursive": false
    }' \
    $GRPC_HOST \
    io.pipeline.repository.filesystem.FilesystemService/DeleteNode

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Delete node successful${NC}"
else
  echo -e "${RED}✗ Delete node failed${NC}"
fi

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Delete node successful${NC}"
  else
    echo -e "${RED}✗ Delete node failed${NC}"
  fi
fi

# Test 9: Verify deletion
echo -e "\n${YELLOW}Test 9: Verifying deletion (should fail)${NC}"

if [ -z "$DOCUMENT_ID" ]; then
  echo -e "${RED}✗ No document ID available for verification${NC}"
else
  grpcurl -plaintext \
    -d '{
      "drive": "'$DRIVE_NAME'",
      "document_id": "'$DOCUMENT_ID'",
      "include_payload": false
    }' \
    $GRPC_HOST \
    io.pipeline.repository.filesystem.FilesystemService/GetNode 2>&1 | grep -q "NOT_FOUND\|not found"

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Node properly deleted (not found)${NC}"
  else
    echo -e "${RED}✗ Node still exists after deletion${NC}"
  fi
fi

echo -e "\n========================================"
echo "CRUD Operations Test Complete"
echo "========================================"