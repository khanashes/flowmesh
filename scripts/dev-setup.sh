#!/bin/bash
set -euo pipefail

echo "🚀 FlowMesh Development Environment Setup"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Go is installed
echo "Checking Go installation..."
if ! command -v go &> /dev/null; then
    echo -e "${RED}✗ Go is not installed${NC}"
    echo "Please install Go 1.21 or later from https://golang.org/doc/install"
    exit 1
fi

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo -e "${GREEN}✓ Go is installed${NC} (version: $GO_VERSION)"

# Check Go version (minimum 1.21)
GO_MAJOR=$(echo $GO_VERSION | cut -d. -f1)
GO_MINOR=$(echo $GO_VERSION | cut -d. -f2)

if [ "$GO_MAJOR" -lt 1 ] || ([ "$GO_MAJOR" -eq 1 ] && [ "$GO_MINOR" -lt 21 ]); then
    echo -e "${RED}✗ Go version 1.21 or later is required${NC}"
    echo "Current version: $GO_VERSION"
    exit 1
fi

# Check if Git is installed
echo ""
echo "Checking Git installation..."
if ! command -v git &> /dev/null; then
    echo -e "${YELLOW}⚠ Git is not installed (optional but recommended)${NC}"
else
    echo -e "${GREEN}✓ Git is installed${NC}"
fi

# Check if Make is installed
echo ""
echo "Checking Make installation..."
if ! command -v make &> /dev/null; then
    echo -e "${YELLOW}⚠ Make is not installed (optional but recommended)${NC}"
    echo "  Install make to use the Makefile commands"
else
    echo -e "${GREEN}✓ Make is installed${NC}"
fi

# Install development tools
echo ""
echo "Installing development tools..."
cd engine

echo "  Installing golangci-lint..."
if ! command -v golangci-lint &> /dev/null; then
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
    echo -e "  ${GREEN}✓ golangci-lint installed${NC}"
else
    echo -e "  ${GREEN}✓ golangci-lint already installed${NC}"
fi

# Download dependencies
echo ""
echo "Downloading Go dependencies..."
go mod download
go mod verify
echo -e "${GREEN}✓ Dependencies downloaded${NC}"

# Check Docker (optional)
echo ""
echo "Checking Docker installation (optional)..."
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}⚠ Docker is not installed (optional)${NC}"
    echo "  Docker is useful for container-based development"
else
    echo -e "${GREEN}✓ Docker is installed${NC}"
fi

echo ""
echo -e "${GREEN}=========================================="
echo "Setup complete! 🎉"
echo "==========================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Run 'make build' to build the FlowMesh binary"
echo "  2. Run 'make test' to run tests"
echo "  3. Run 'make dev' to start FlowMesh in development mode"
echo "  4. Read CONTRIBUTING.md for development guidelines"
echo ""

