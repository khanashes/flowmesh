#!/bin/bash
# Quick test to verify replay endpoints are working

API_URL="${API_URL:-http://localhost:8080}"
STREAM="${STREAM:-default/test/events}"

echo "Quick Replay API Test"
echo "===================="
echo ""

# Check if API is reachable
echo "1. Checking API health..."
HEALTH=$(curl -s "$API_URL/health")
if [ $? -eq 0 ]; then
  echo "✓ API is reachable"
else
  echo "✗ API is not reachable. Make sure FlowMesh is running."
  exit 1
fi
echo ""

# Check if stream exists (create events if needed)
echo "2. Ensuring stream has events..."
LATEST_OFFSET=$(curl -s "$API_URL/api/v1/streams/$STREAM/offsets/latest?partition=0" | python3 -c "import sys, json; print(json.load(sys.stdin).get('offset', 0))" 2>/dev/null || echo "0")
if [ "$LATEST_OFFSET" = "0" ] || [ -z "$LATEST_OFFSET" ]; then
  echo "  Adding test events..."
  for i in {1..5}; do
    PAYLOAD=$(echo -n "Test event $i" | base64)
    curl -s -X POST "$API_URL/api/v1/streams/$STREAM/events" \
      -H 'Content-Type: application/json' \
      -d "{\"events\": [{\"payload\": \"$PAYLOAD\"}]}" > /dev/null
  done
  echo "  ✓ Added 5 test events"
else
  echo "  ✓ Stream has events (latest offset: $LATEST_OFFSET)"
fi
echo ""

# Test creating a replay session
echo "3. Testing create replay session..."
CREATE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/replay/sessions?stream=$STREAM" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_offset": 0,
    "sandbox_consumer_group": "quick-test-group"
  }')

SESSION_ID=$(echo "$CREATE_RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('session', {}).get('session_id', ''))" 2>/dev/null)

if [ ! -z "$SESSION_ID" ]; then
  echo "✓ Session created: $SESSION_ID"
else
  echo "✗ Failed to create session"
  echo "$CREATE_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$CREATE_RESPONSE"
  exit 1
fi
echo ""

# Test listing sessions
echo "4. Testing list sessions..."
LIST_RESPONSE=$(curl -s "$API_URL/api/v1/replay/sessions")
SESSION_COUNT=$(echo "$LIST_RESPONSE" | python3 -c "import sys, json; print(len(json.load(sys.stdin).get('sessions', [])))" 2>/dev/null || echo "0")
if [ "$SESSION_COUNT" -gt 0 ]; then
  echo "✓ Found $SESSION_COUNT session(s)"
else
  echo "✗ No sessions found"
  echo "$LIST_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$LIST_RESPONSE"
  exit 1
fi
echo ""

# Test getting session details
echo "5. Testing get session details..."
GET_RESPONSE=$(curl -s "$API_URL/api/v1/replay/sessions/$SESSION_ID")
STATUS=$(echo "$GET_RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('session', {}).get('status', ''))" 2>/dev/null)
if [ ! -z "$STATUS" ]; then
  echo "✓ Session status: $STATUS"
else
  echo "✗ Failed to get session"
  echo "$GET_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$GET_RESPONSE"
  exit 1
fi
echo ""

echo "===================="
echo "✓ All endpoint tests passed!"
echo ""
echo "Session ID: $SESSION_ID"
echo "View in UI: http://localhost:8080/replay"
echo ""
echo "To test full workflow, run:"
echo "  ./scripts/test-replay-endpoints.sh"
echo ""
echo "To create multiple test sessions for UI:"
echo "  ./scripts/create-test-replay-sessions.sh"

