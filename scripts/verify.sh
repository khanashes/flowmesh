#!/bin/bash
set -euo pipefail

echo "🔍 Verifying FlowMesh Development Environment"
echo "=============================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

EXIT_CODE=0

# Function to check and report
check() {
    local name=$1
    local command=$2
    
    if eval "$command" &> /dev/null; then
        echo -e "${GREEN}✓${NC} $name"
        return 0
    else
        echo -e "${RED}✗${NC} $name"
        return 1
    fi
}

# Check Go
echo "Go Environment:"
if check "Go is installed" "command -v go"; then
    GO_VERSION=$(go version | awk '{print $3}')
    echo "  Version: $GO_VERSION"
    
    # Check minimum version
    GO_MAJOR=$(go version | awk '{print $3}' | sed 's/go//' | cut -d. -f1)
    GO_MINOR=$(go version | awk '{print $3}' | sed 's/go//' | cut -d. -f2)
    
    if [ "$GO_MAJOR" -lt 1 ] || ([ "$GO_MAJOR" -eq 1 ] && [ "$GO_MINOR" -lt 21 ]); then
        echo -e "  ${RED}✗ Go 1.21 or later is required${NC}"
        EXIT_CODE=1
    else
        echo -e "  ${GREEN}✓ Version meets requirements${NC}"
    fi
else
    EXIT_CODE=1
fi

echo ""

# Check GOPATH and GOROOT
echo "Go Configuration:"
if [ -n "${GOPATH:-}" ]; then
    echo "  GOPATH: $GOPATH"
else
    echo "  GOPATH: (not set, using default)"
fi

if [ -n "${GOROOT:-}" ]; then
    echo "  GOROOT: $GOROOT"
else
    echo "  GOROOT: (not set, using default)"
fi

echo ""

# Check required commands
echo "Required Tools:"
check "Git is installed" "command -v git" || EXIT_CODE=1
check "Make is installed" "command -v make" || EXIT_CODE=1

echo ""

# Check optional tools
echo "Optional Tools:"
if ! check "Docker is installed" "command -v docker"; then
    echo -e "  ${YELLOW}  (optional, for container development)${NC}"
fi
if ! check "golangci-lint is installed" "command -v golangci-lint"; then
    echo -e "  ${YELLOW}  (optional, install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)${NC}"
fi

echo ""

# Check project structure
echo "Project Structure:"
check "engine directory exists" "[ -d engine ]" || EXIT_CODE=1
check "go.mod exists" "[ -f engine/go.mod ]" || EXIT_CODE=1
check "Makefile exists" "[ -f Makefile ]" || EXIT_CODE=1

echo ""

# Verify Go dependencies
echo "Go Dependencies:"
cd engine
if go mod verify &> /dev/null; then
    echo -e "${GREEN}✓ Dependencies are valid${NC}"
else
    echo -e "${RED}✗ Dependencies verification failed${NC}"
    echo "  Run: go mod download && go mod verify"
    EXIT_CODE=1
fi

cd ..

echo ""

# Check if code compiles
echo "Code Compilation:"
cd engine
if go build ./cmd/flowmesh &> /dev/null; then
    echo -e "${GREEN}✓ Code compiles successfully${NC}"
    rm -f flowmesh 2>/dev/null || true
else
    echo -e "${RED}✗ Code compilation failed${NC}"
    echo "  Run: go build ./cmd/flowmesh"
    EXIT_CODE=1
fi
cd ..

echo ""

# Summary
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=============================================="
    echo "✓ All checks passed!"
    echo "==============================================${NC}"
else
    echo -e "${RED}=============================================="
    echo "✗ Some checks failed"
    echo "==============================================${NC}"
fi

exit $EXIT_CODE

