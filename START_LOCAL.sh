#!/bin/bash
# Quick start script for FlowMesh local development

set -e

echo "🚀 Starting FlowMesh Local Development Environment"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Go
if command -v go &> /dev/null; then
    GO_VERSION=$(go version | awk '{print $3}')
    echo -e "${GREEN}✓${NC} Go found: $GO_VERSION"
else
    echo -e "${RED}✗${NC} Go not found. Please install Go 1.24+"
    exit 1
fi

# Check Node.js
if command -v node &> /dev/null; then
    NODE_VERSION=$(node --version | sed 's/v//')
    MAJOR=$(echo $NODE_VERSION | cut -d. -f1)
    MINOR=$(echo $NODE_VERSION | cut -d. -f2)
    
    if [ "$MAJOR" -eq "20" ] && [ "$MINOR" -lt "19" ]; then
        echo -e "${YELLOW}⚠${NC} Node.js version $NODE_VERSION found, but Vite requires 20.19+ or 22.12+"
        echo "   Consider upgrading Node.js or using Docker instead"
        echo ""
        read -p "Continue anyway? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo -e "${GREEN}✓${NC} Node.js found: v$NODE_VERSION"
    fi
else
    echo -e "${RED}✗${NC} Node.js not found. Please install Node.js 20.19+ or 22.12+"
    exit 1
fi

echo ""
echo "🔨 Building FlowMesh binary..."
cd engine
go build -o ../bin/flowmesh ./cmd/flowmesh
echo -e "${GREEN}✓${NC} Binary built successfully"
cd ..

echo ""
echo "📦 Checking Web UI dependencies..."
cd web-ui
if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
else
    echo -e "${GREEN}✓${NC} Dependencies already installed"
fi
cd ..

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Setup complete!"
echo ""
echo "To start the services, run these commands in separate terminals:"
echo ""
echo "Terminal 1 (Backend):"
echo "  cd $(pwd)"
echo "  ./bin/flowmesh"
echo ""
echo "Terminal 2 (Frontend):"
echo "  cd $(pwd)/web-ui"
echo "  npm run dev"
echo ""
echo "Then open http://localhost:5173 in your browser"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
