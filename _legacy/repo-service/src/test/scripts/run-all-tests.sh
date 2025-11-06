#!/bin/bash
# Run all repo-service test scripts

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED_TESTS=""
PASSED_TESTS=""

echo -e "${BLUE}========================================"
echo "   Repository Service Test Suite"
echo -e "========================================${NC}"
echo

# Function to run a test script
run_test() {
    local test_name=$1
    local test_script=$2

    echo -e "\n${YELLOW}Running: $test_name${NC}"
    echo "----------------------------------------"

    if [ -f "$SCRIPT_DIR/$test_script" ]; then
        bash "$SCRIPT_DIR/$test_script" > /tmp/test_output.log 2>&1
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ $test_name PASSED${NC}"
            PASSED_TESTS="$PASSED_TESTS\n  ✓ $test_name"
            # Show summary of test
            tail -5 /tmp/test_output.log | grep -E "✓|Complete" | head -3
        else
            echo -e "${RED}✗ $test_name FAILED${NC}"
            FAILED_TESTS="$FAILED_TESTS\n  ✗ $test_name"
            # Show error from test
            tail -10 /tmp/test_output.log | grep -E "ERROR|Failed|✗" | head -3
        fi
    else
        echo -e "${RED}✗ Test script not found: $test_script${NC}"
        FAILED_TESTS="$FAILED_TESTS\n  ✗ $test_name (script not found)"
    fi
}

# Run all tests
run_test "CRUD Operations" "test-crud-operations.sh"
run_test "Small File Upload" "test-small-file.sh"
run_test "Chunked Upload" "test-chunked-upload.sh"
run_test "Client-Provided IDs" "test-client-provided-ids.sh"
run_test "Download/Stream" "test-download-stream.sh"

# Summary
echo -e "\n${BLUE}========================================"
echo "           Test Summary"
echo -e "========================================${NC}"

if [ -n "$PASSED_TESTS" ]; then
    echo -e "\n${GREEN}Passed Tests:${NC}"
    echo -e "$PASSED_TESTS"
fi

if [ -n "$FAILED_TESTS" ]; then
    echo -e "\n${RED}Failed Tests:${NC}"
    echo -e "$FAILED_TESTS"
    exit 1
else
    echo -e "\n${GREEN}All tests passed successfully!${NC}"
fi

echo
echo -e "${BLUE}========================================${NC}"