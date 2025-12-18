# Replay API Testing

This document describes how to test the Replay API endpoints and UI.

## Prerequisites

1. FlowMesh server running (either via Docker or locally)
2. At least one stream with some events (use `scripts/add-test-stream-data.sh`)

## Test Scripts

### 1. `test-replay-endpoints.sh`

Comprehensive test script that exercises all replay API endpoints:

```bash
./scripts/test-replay-endpoints.sh
```

This script:
- Creates replay sessions with offset and timestamp-based start positions
- Lists all sessions
- Gets session details
- Tests start/pause/resume/stop operations
- Tests delete operation
- Filters sessions by stream

**Environment Variables:**
- `API_URL` - API base URL (default: `http://localhost:8080`)
- `STREAM` - Stream to test with (default: `default/test/events`)

**Example:**
```bash
API_URL=http://localhost:8080 STREAM=default/test/logs ./scripts/test-replay-endpoints.sh
```

### 2. `create-test-replay-sessions.sh`

Creates multiple test replay sessions with different configurations for UI testing:

```bash
./scripts/create-test-replay-sessions.sh
```

This script creates:
- Session 1: Created but not started (offset-based, no end)
- Session 2: Active session (offset-based with end, started)
- Session 3: Paused session (timestamp-based, started then paused)
- Session 4: Completed/active session (offset-based with small range)

**Environment Variables:**
- `API_URL` - API base URL (default: `http://localhost:8080`)
- `STREAM` - Stream to create sessions for (default: `default/test/events`)

**Example:**
```bash
API_URL=http://localhost:8080 STREAM=production/metrics/telemetry ./scripts/create-test-replay-sessions.sh
```

## API Endpoints

### Create Replay Session

```bash
curl -X POST "http://localhost:8080/api/v1/replay/sessions?stream=default/test/events" \
  -H "Content-Type: application/json" \
  -d '{
    "start_offset": 0,
    "end_offset": 10,
    "sandbox_consumer_group": "test-group"
  }'
```

### List Replay Sessions

```bash
# List all sessions
curl "http://localhost:8080/api/v1/replay/sessions"

# Filter by stream
curl "http://localhost:8080/api/v1/replay/sessions?stream=default/test/events"
```

### Get Session Details

```bash
curl "http://localhost:8080/api/v1/replay/sessions/{session_id}"
```

### Control Replay

```bash
# Start
curl -X POST "http://localhost:8080/api/v1/replay/sessions/{session_id}/start"

# Pause
curl -X POST "http://localhost:8080/api/v1/replay/sessions/{session_id}/pause"

# Resume
curl -X POST "http://localhost:8080/api/v1/replay/sessions/{session_id}/resume"

# Stop
curl -X POST "http://localhost:8080/api/v1/replay/sessions/{session_id}/stop"

# Delete
curl -X DELETE "http://localhost:8080/api/v1/replay/sessions/{session_id}"
```

## Testing in UI

1. **Prepare test data:**
   ```bash
   # Create streams if needed
   ./scripts/create-test-streams.sh
   
   # Add events to streams
   ./scripts/add-test-stream-data.sh
   
   # Create test replay sessions
   ./scripts/create-test-replay-sessions.sh
   ```

2. **Open the UI:**
   - Navigate to `http://localhost:8080/replay`
   - You should see the list of replay sessions

3. **Test UI features:**
   - Create new session via the form
   - Filter sessions by stream
   - Select a session to view details
   - Use control buttons (start/pause/resume/stop/delete)
   - Watch progress updates in real-time

## Troubleshooting

### "stream query parameter is required"
Make sure to include the `stream` query parameter when creating sessions:
```bash
curl -X POST "http://localhost:8080/api/v1/replay/sessions?stream=default/test/events" ...
```

### "either start_offset or start_timestamp must be provided"
You must provide either `start_offset` or `start_timestamp` in the request body.

### Sessions not showing in UI
- Check that the API is returning sessions: `curl http://localhost:8080/api/v1/replay/sessions`
- Check browser console for errors
- Verify the stream exists: `curl http://localhost:8080/api/v1/streams`

### Session status not updating
- Progress polling happens every 2 seconds for active/paused sessions
- Make sure the replay manager is running and processing events

