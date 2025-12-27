# WebSocket Testing Guide

This guide explains how to test the WebSocket implementation for real-time updates.

## Prerequisites

1. Start the FlowMesh server:
   ```bash
   docker-compose up
   ```

2. The server should be running on `http://localhost:8080`

## Testing WebSocket Endpoint

### 1. Manual Test with Browser Console

1. Open the FlowMesh Web UI in your browser
2. Open the browser developer console (F12)
3. Check the WebSocket connection status in the header (should show "Live" with green indicator)

### 2. Test WebSocket Connection

You can test the WebSocket endpoint directly using various tools:

#### Using wscat (Node.js)
```bash
npm install -g wscat
wscat -c ws://localhost:8080/ws
```

Then send a subscription message:
```json
{"type":"subscribe","payload":{"topics":["stream.stats.default/test/logs"]}}
```

#### Using websocat (Rust)
```bash
# Install: cargo install websocat
websocat ws://localhost:8080/ws
```

#### Using the test script
```bash
./test-websocket.sh
```

## Testing Real-time Updates

### Stream Stats Updates

1. Open the Streams page in the Web UI
2. Select a stream to view its stats
3. Write events to the stream using the API:
   ```bash
   curl -X POST http://localhost:8080/api/v1/streams/default/test/logs/events \
     -H "Content-Type: application/json" \
     -d '{"events":[{"payload":"dGVzdCBldmVudA=="}]}'
   ```
4. The stream stats should update automatically in the UI without page refresh

### Queue Stats Updates

1. Open the Queues page in the Web UI
2. Select a queue to view its stats
3. Enqueue a job:
   ```bash
   curl -X POST http://localhost:8080/api/v1/queues/default/test/priority-queue/enqueue \
     -H "Content-Type: application/json" \
     -d '{"payload":"dGVzdCBqb2I="}'
   ```
4. The queue stats should update automatically

### Replay Session Updates

1. Open the Replay page in the Web UI
2. Create a replay session
3. Start the replay session
4. The session status and progress should update in real-time

### KV Store Updates

1. Open the KV Store page in the Web UI
2. Select a KV store and a key
3. Update the key value
4. The UI should reflect the changes immediately

## Expected Behavior

1. **WebSocket Connection**: 
   - Header shows green "Live" indicator when connected
   - Shows yellow "Connecting..." when establishing connection
   - Shows red "Offline" when disconnected (fallback to polling)

2. **Real-time Updates**:
   - Stats update automatically without page refresh
   - No need to manually refresh the page
   - Polling is disabled when WebSocket is connected

3. **Fallback Behavior**:
   - If WebSocket disconnects, the UI automatically falls back to polling
   - Polling interval resumes based on the hook configuration

## Debugging

### Check WebSocket Connection

In browser console:
```javascript
// Check if WebSocket is connected
// The useWebSocket hook automatically manages connection

// Monitor WebSocket messages
// Open Network tab → WS filter → Select the WebSocket connection
```

### Server Logs

Check server logs for WebSocket connection messages:
```bash
docker-compose logs -f engine
```

Look for:
- `Client registered` - New WebSocket client connected
- `Client unsubscribed` - Client disconnected
- `Broadcast channel full` - Too many messages queued (shouldn't happen normally)

### Common Issues

1. **WebSocket not connecting**:
   - Check if server is running
   - Verify endpoint: `ws://localhost:8080/ws` (or `wss://` for HTTPS)
   - Check browser console for errors

2. **Updates not appearing**:
   - Verify the topic subscription matches the broadcast topic
   - Check that the handler is broadcasting (see server logs)
   - Ensure the WebSocket is connected (check header indicator)

3. **Polling still active**:
   - This is expected if WebSocket is not connected
   - Check connection status in header
   - Verify WebSocket URL is correct in config

## WebSocket Topics

The following topics are used for real-time updates:

- `stream.stats.{tenant}/{namespace}/{name}` - Stream statistics
- `queue.stats.{tenant}/{namespace}/{name}` - Queue statistics  
- `replay.session.{sessionId}` - Replay session updates
- `kv.update.{tenant}/{namespace}/{name}` - KV store updates

## Message Format

WebSocket messages follow this format:
```json
{
  "type": "stream.stats",
  "topic": "stream.stats.default/test/logs",
  "payload": {
    "latest_offset": 1234
  }
}
```

## Testing Broadcasts

You can manually trigger broadcasts by calling the corresponding API endpoints:

```bash
# This will broadcast stream stats update
curl http://localhost:8080/api/v1/streams/default/test/logs/stats

# This will broadcast queue stats update
curl http://localhost:8080/api/v1/queues/default/test/priority-queue/stats

# This will broadcast replay session update
curl http://localhost:8080/api/v1/replay/sessions/{sessionId}
```

