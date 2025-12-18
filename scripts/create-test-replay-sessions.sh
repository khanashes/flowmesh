#!/bin/bash
# Script to create test replay sessions for the UI

API_URL="${API_URL:-http://localhost:8080}"
STREAM="${STREAM:-default/test/events}"

echo "Creating test replay sessions for UI testing..."
echo "Stream: $STREAM"
echo ""

# Ensure stream has some events first
echo "Adding some test events to stream..."
for i in {1..10}; do
  PAYLOAD=$(echo -n "Test event $i for replay" | base64)
  curl -s -X POST "$API_URL/api/v1/streams/$STREAM/events" \
    -H 'Content-Type: application/json' \
    -d "{\"events\": [{\"payload\": \"$PAYLOAD\"}]}" > /dev/null
done
echo "✓ Added 10 test events"
echo ""

# Create session 1: Offset-based, no end
echo "Creating session 1: Offset-based (start: 0, no end)..."
SESSION1=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_offset": 0,
    "sandbox_consumer_group": "test-replay-group-1"
  }')

SESSION1_ID=$(echo "$SESSION1" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)
if [ ! -z "$SESSION1_ID" ]; then
  echo "  ✓ Created: $SESSION1_ID"
else
  echo "  ✗ Failed: $SESSION1"
fi
echo ""

# Create session 2: Offset-based with end
echo "Creating session 2: Offset-based (start: 0, end: 5)..."
SESSION2=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_offset": 0,
    "end_offset": 5,
    "sandbox_consumer_group": "test-replay-group-2"
  }')

SESSION2_ID=$(echo "$SESSION2" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)
if [ ! -z "$SESSION2_ID" ]; then
  echo "  ✓ Created: $SESSION2_ID"
  
  # Start this session
  echo "  Starting session..."
  curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION2_ID/start" > /dev/null
  echo "  ✓ Started"
else
  echo "  ✗ Failed: $SESSION2"
fi
echo ""

# Create session 3: Timestamp-based
echo "Creating session 3: Timestamp-based..."
TIMESTAMP_NOW=$(date +%s%N)
TIMESTAMP_START=$((TIMESTAMP_NOW - 3600000000000)) # 1 hour ago

SESSION3=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d "{
    \"start_timestamp\": $TIMESTAMP_START,
    \"sandbox_consumer_group\": \"test-replay-group-3\"
  }")

SESSION3_ID=$(echo "$SESSION3" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)
if [ ! -z "$SESSION3_ID" ]; then
  echo "  ✓ Created: $SESSION3_ID"
  
  # Start and pause this session
  echo "  Starting and pausing session..."
  curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION3_ID/start" > /dev/null
  sleep 1
  curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION3_ID/pause" > /dev/null
  echo "  ✓ Started and paused"
else
  echo "  ✗ Failed: $SESSION3"
fi
echo ""

# Create session 4: Offset-based, will be completed
echo "Creating session 4: Offset-based (start: 0, end: 3) - will be completed..."
SESSION4=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_offset": 0,
    "end_offset": 3,
    "sandbox_consumer_group": "test-replay-group-4"
  }')

SESSION4_ID=$(echo "$SESSION4" | python3 -c "import sys, json; print(json.load(sys.stdin)['session']['session_id'])" 2>/dev/null)
if [ ! -z "$SESSION4_ID" ]; then
  echo "  ✓ Created: $SESSION4_ID"
  
  # Start and let it complete
  echo "  Starting session (will complete quickly)..."
  curl -s -X POST "$API_URL/api/v1/replay/sessions/$SESSION4_ID/start" > /dev/null
  sleep 2
  echo "  ✓ Session should be completed or active"
else
  echo "  ✗ Failed: $SESSION4"
fi
echo ""

echo "============================="
echo "Test replay sessions created!"
echo ""
echo "Session IDs:"
[ ! -z "$SESSION1_ID" ] && echo "  - $SESSION1_ID (created, not started)"
[ ! -z "$SESSION2_ID" ] && echo "  - $SESSION2_ID (active)"
[ ! -z "$SESSION3_ID" ] && echo "  - $SESSION3_ID (paused)"
[ ! -z "$SESSION4_ID" ] && echo "  - $SESSION4_ID (completed/active)"
echo ""
echo "View in UI: http://localhost:8080/replay"
echo ""
echo "List all sessions:"
echo "  curl -s $API_URL/api/v1/replay/sessions | python3 -m json.tool"

