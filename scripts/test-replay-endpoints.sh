#!/bin/bash
# Script to test replay API endpoints

API_URL="${API_URL:-http://localhost:8080}"
STREAM="${STREAM:-default/test/events}"

echo "Testing Replay API Endpoints"
echo "============================="
echo "API URL: $API_URL"
echo "Test Stream: $STREAM"
echo ""

# Check if stream exists and has events
echo "1. Checking stream stats..."
STREAM_STATS=$(curl -s "$API_URL/api/v1/streams/$STREAM/stats")
echo "$STREAM_STATS" | python3 -m json.tool 2>/dev/null || echo "$STREAM_STATS"
echo ""

# Create a replay session with offset
echo "2. Creating replay session with start offset 0..."
SESSION_CREATE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_offset": 0,
    "sandbox_consumer_group": "test-replay-group-1"
  }')

SESSION_ID=$(echo "$SESSION_CREATE" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)

if [ -z "$SESSION_ID" ]; then
  echo "Failed to create session:"
  echo "$SESSION_CREATE" | python3 -m json.tool 2>/dev/null || echo "$SESSION_CREATE"
  exit 1
fi

echo "✓ Session created: $SESSION_ID"
echo "$SESSION_CREATE" | python3 -m json.tool 2>/dev/null || echo "$SESSION_CREATE"
echo ""

# List all replay sessions
echo "3. Listing all replay sessions..."
LIST_RESPONSE=$(curl -s "$API_URL/api/v1/replay/sessions")
echo "$LIST_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$LIST_RESPONSE"
echo ""

# Get specific session
echo "4. Getting session details for $SESSION_ID..."
GET_SESSION=$(curl -s "$API_URL/api/v1/replay/sessions/$SESSION_ID")
echo "$GET_SESSION" | python3 -m json.tool 2>/dev/null || echo "$GET_SESSION"
echo ""

# Start the replay
echo "5. Starting replay for session $SESSION_ID..."
START_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION_ID/start")
echo "$START_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$START_RESPONSE"
echo ""

# Wait a bit and check progress
sleep 2
echo "6. Checking replay progress..."
GET_SESSION_AFTER=$(curl -s "$API_URL/api/v1/replay/sessions/$SESSION_ID")
echo "$GET_SESSION_AFTER" | python3 -m json.tool 2>/dev/null || echo "$GET_SESSION_AFTER"
echo ""

# Pause the replay
echo "7. Pausing replay for session $SESSION_ID..."
PAUSE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION_ID/pause")
echo "$PAUSE_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$PAUSE_RESPONSE"
echo ""

# Resume the replay
echo "8. Resuming replay for session $SESSION_ID..."
RESUME_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION_ID/resume")
echo "$RESUME_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESUME_RESPONSE"
echo ""

# Wait a bit and check progress again
sleep 2
echo "9. Checking replay progress after resume..."
GET_SESSION_PROGRESS=$(curl -s "$API_URL/api/v1/replay/sessions/$SESSION_ID")
echo "$GET_SESSION_PROGRESS" | python3 -m json.tool 2>/dev/null || echo "$GET_SESSION_PROGRESS"
echo ""

# Stop the replay
echo "10. Stopping replay for session $SESSION_ID..."
STOP_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION_ID/stop")
echo "$STOP_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$STOP_RESPONSE"
echo ""

# Create another session with timestamp
echo "11. Creating replay session with timestamp..."
TIMESTAMP_NS=$(date +%s%N)
TIMESTAMP_START=$((TIMESTAMP_NS - 3600000000000)) # 1 hour ago
TIMESTAMP_END=$TIMESTAMP_NS

SESSION_CREATE_TIMESTAMP=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d "{
    \"start_timestamp\": $TIMESTAMP_START,
    \"end_timestamp\": $TIMESTAMP_END,
    \"sandbox_consumer_group\": \"test-replay-group-2\"
  }")

SESSION_ID_2=$(echo "$SESSION_CREATE_TIMESTAMP" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)

if [ ! -z "$SESSION_ID_2" ]; then
  echo "✓ Session created with timestamp: $SESSION_ID_2"
  echo "$SESSION_CREATE_TIMESTAMP" | python3 -m json.tool 2>/dev/null || echo "$SESSION_CREATE_TIMESTAMP"
  echo ""

  # Delete the second session
  echo "12. Deleting session $SESSION_ID_2..."
  DELETE_RESPONSE=$(curl -s -X DELETE "$API_URL/api/v1/replay/sessions/$SESSION_ID_2")
  echo "$DELETE_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$DELETE_RESPONSE"
  echo ""
fi

# List sessions filtered by stream
echo "13. Listing sessions filtered by stream..."
LIST_FILTERED=$(curl -s "$API_URL/api/v1/replay/sessions?stream=$STREAM")
echo "$LIST_FILTERED" | python3 -m json.tool 2>/dev/null || echo "$LIST_FILTERED"
echo ""

# Final list of all sessions
echo "14. Final list of all replay sessions..."
FINAL_LIST=$(curl -s "$API_URL/api/v1/replay/sessions")
echo "$FINAL_LIST" | python3 -m json.tool 2>/dev/null || echo "$FINAL_LIST"
echo ""

echo "============================="
echo "Test complete!"
echo ""
echo "Session IDs created:"
echo "  - $SESSION_ID (with offset)"
if [ ! -z "$SESSION_ID_2" ]; then
  echo "  - $SESSION_ID_2 (with timestamp, deleted)"
fi
echo ""
echo "You can view these sessions in the Web UI at: http://localhost:8080/replay"

