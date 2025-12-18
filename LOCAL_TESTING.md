# Local Testing Guide for FlowMesh Web UI

## Quick Start: Run and Test Locally

### Prerequisites

- **Go 1.24+** (for backend)
- **Node.js 20.19+ or 22.12+** (for frontend)
- **npm 10+**

Check your versions:
```bash
go version    # Should be 1.24+
node --version # Should be 20.19+ or 22.12+
npm --version  # Should be 10+
```

## Option 1: Development Mode (Recommended for Testing)

This setup runs the backend and frontend separately, providing hot reload for both.

### Step 1: Build and Start the FlowMesh Backend

**Terminal 1:**

```bash
# Navigate to project root
cd /Users/eapple/Desktop/Open\ source/FlowMesh

# Build the binary
cd engine
go build -o ../bin/flowmesh ./cmd/flowmesh

# Start the FlowMesh server
../bin/flowmesh
```

The server will start and show:
- ✅ gRPC server on `:50051`
- ✅ HTTP/REST API on `:8080`
- ✅ Metrics on `:9090`

You should see logs like:
```
{"level":"info","component":"flowmesh","message":"FlowMesh server started"}
{"level":"info","component":"api","message":"API server started"}
{"level":"info","component":"grpc","addr":":50051","message":"gRPC server started"}
{"level":"info","component":"http","addr":":8080","message":"HTTP server started"}
```

### Step 2: Start the Web UI Dev Server

**Terminal 2** (new terminal window):

```bash
# Navigate to web-ui directory
cd /Users/eapple/Desktop/Open\ source/FlowMesh/web-ui

# Install dependencies (if not already installed)
npm install

# Start the development server
npm run dev
```

The dev server will start on `http://localhost:5173` with hot reload enabled.

You should see:
```
  VITE v7.x.x  ready in xxx ms

  ➜  Local:   http://localhost:5173/
  ➜  Network: use --host to expose
```

### Step 3: Open in Browser

Open your browser and navigate to:
- **Web UI**: http://localhost:5173
- **API Health Check**: http://localhost:8080/health
- **API Readiness**: http://localhost:8080/ready

The Web UI will automatically connect to the backend API at `http://localhost:8080`.

### Step 4: Test the Web UI

1. **Health Page**: 
   - Click on "Health" in the sidebar
   - You should see real-time health and readiness status
   - The status should update every 5 seconds automatically

2. **Dashboard**: 
   - Click on "Dashboard" to see the main page

3. **Other Pages**: 
   - Streams, Queues, KV, and Replay pages are currently placeholders

### Step 5: Verify It's Working

Check browser console (F12 → Console tab):
- ✅ No red errors
- ✅ Network requests to `http://localhost:8080/api/v1/...` are successful

Check Network tab (F12 → Network):
- ✅ Health check requests return 200 OK
- ✅ API responses are JSON

## Option 2: Docker Mode (Production-like)

If you want to test the production build with everything bundled:

### Step 1: Build and Run with Docker

```bash
# Navigate to project root
cd /Users/eapple/Desktop/Open\ source/FlowMesh

# Build the Docker image (includes Web UI)
make docker-build

# Run the container
docker-compose up -d

# View logs
docker-compose logs -f
```

### Step 2: Access Services

- **Web UI**: http://localhost:8080
- **API**: http://localhost:8080/api/v1/...
- **Health**: http://localhost:8080/health

The Web UI is served directly by the Go server in this mode.

### Step 3: Stop Container

```bash
docker-compose down
```

## Troubleshooting

### Backend Issues

**Port 8080 already in use:**
```bash
# Find what's using the port
lsof -i :8080

# Kill the process or use a different port
export SERVER_HTTP_ADDR=:8081
../bin/flowmesh
```

**Go version too old:**
```bash
# Update Go to 1.24+
# Or use Docker instead
```

**Binary not found:**
```bash
# Make sure you built it first
cd engine
go build -o ../bin/flowmesh ./cmd/flowmesh
```

### Frontend Issues

**Node.js version too old:**
- Required: Node.js 20.19+ or 22.12+
- Current issue: You have Node.js 20.9.0
- Solution: Upgrade Node.js using nvm or download from nodejs.org

**npm install fails:**
```bash
# Clear npm cache
npm cache clean --force

# Delete node_modules and reinstall
rm -rf node_modules package-lock.json
npm install
```

**Port 5173 already in use:**
```bash
# Vite will automatically try the next port (5174, 5175, etc.)
# Or specify a different port:
npm run dev -- --port 3000
```

**CORS errors:**
- The backend should handle CORS automatically
- If you see CORS errors, make sure the backend is running on port 8080

**Web UI not connecting to backend:**
- Check that backend is running: `curl http://localhost:8080/health`
- Check browser console for error messages
- Verify API_BASE_URL in `web-ui/src/lib/config.ts` (default: `http://localhost:8080`)

## Testing Checklist

- [ ] Backend server starts successfully (Terminal 1 shows "server started")
- [ ] Web UI dev server starts successfully (Terminal 2 shows Vite ready)
- [ ] Can access Web UI at http://localhost:5173
- [ ] Health page shows status correctly (green "OK" badges)
- [ ] No console errors in browser (F12 → Console)
- [ ] API calls are working (F12 → Network tab shows 200 responses)
- [ ] Health status updates every 5 seconds automatically

## Next Steps

Once you have it running:
1. Test the Health page functionality
2. Explore the UI components
3. Check the API client in `web-ui/src/lib/api.ts`
4. Review the React components in `web-ui/src/components/`
5. Add new features to the placeholder pages
