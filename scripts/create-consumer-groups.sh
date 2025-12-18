#!/bin/bash
# Script to create consumer groups with committed offsets for testing the Stream Dashboard

API_URL="${API_URL:-http://localhost:8080}"

echo "Creating consumer groups with offsets..."
echo ""

# For default/test/events stream (latest offset is 4)
# Create consumer group 1 - committed at offset 2, lag = 2 (4 - 2)
echo "Creating consumer group 'test-group-1' for default/test/events (offset: 2, lag: 2)..."
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/streams/default/test/events/consumer-groups/test-group-1/offsets" \
  -H 'Content-Type: application/json' \
  -d '{"offset": 2}')
if echo "$RESPONSE" | grep -q "success\|status"; then
  echo "  ✓ Consumer group 'test-group-1' created"
else
  echo "  ✗ Failed: $RESPONSE"
fi

# Create consumer group 2 - committed at offset 1, lag = 3 (4 - 1)
echo "Creating consumer group 'test-group-2' for default/test/events (offset: 1, lag: 3)..."
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/streams/default/test/events/consumer-groups/test-group-2/offsets" \
  -H 'Content-Type: application/json' \
  -d '{"offset": 1}')
if echo "$RESPONSE" | grep -q "success\|status"; then
  echo "  ✓ Consumer group 'test-group-2' created"
else
  echo "  ✗ Failed: $RESPONSE"
fi

# For default/test/logs stream (latest offset is 2)
# Create consumer group - committed at offset 0, lag = 2 (2 - 0)
echo "Creating consumer group 'log-processor' for default/test/logs (offset: 0, lag: 2)..."
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/streams/default/test/logs/consumer-groups/log-processor/offsets" \
  -H 'Content-Type: application/json' \
  -d '{"offset": 0}')
if echo "$RESPONSE" | grep -q "success\|status"; then
  echo "  ✓ Consumer group 'log-processor' created"
else
  echo "  ✗ Failed: $RESPONSE"
fi

echo ""
echo "Done! Check the Stream Dashboard to see consumer groups with lag information."
echo ""
echo "View consumer groups:"
echo "  curl $API_URL/api/v1/streams/default/test/events/consumer-groups"
echo "  curl $API_URL/api/v1/streams/default/test/logs/consumer-groups"

