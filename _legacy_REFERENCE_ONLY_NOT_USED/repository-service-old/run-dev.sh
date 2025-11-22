#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting PipeDoc Repository Service in Development Mode${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}Service URLs:${NC}"
echo -e "  HTTP/gRPC: http://localhost:38002"
echo -e "  Health:    http://localhost:38002/q/health"
echo -e "  Swagger:   http://localhost:38002/q/swagger-ui"
echo -e "  Redis:     localhost:6379 (auto-managed by Dev Services)"
echo ""
echo -e "${YELLOW}gRPC Services:${NC}"
echo -e "  - FilesystemService"
echo -e "  - PipeDocRepositoryService"
echo -e "  - GenericRepositoryService"
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Run Quarkus in dev mode
gradle quarkusDev