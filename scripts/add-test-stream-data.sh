#!/bin/bash
# Script to add test events to streams for testing the Stream Dashboard

API_URL="${API_URL:-http://localhost:8080}"

echo "Adding test events to streams..."
echo ""

# Add events to default/test/events stream
echo "Adding events to default/test/events..."
for i in {1..5}; do
  PAYLOAD=$(echo -n "Event $i - $(date)" | base64)
  RESPONSE=$(curl -s -X POST "$API_URL/api/v1/streams/default/test/events/events" \
    -H 'Content-Type: application/json' \
    -d "{\"events\": [{\"payload\": \"$PAYLOAD\"}]}")
  if [ $? -eq 0 ]; then
    echo "  ✓ Event $i added"
  else
    echo "  ✗ Failed to add event $i"
  fi
done

echo ""
echo "Adding events to default/test/logs..."
for i in {1..3}; do
  PAYLOAD=$(echo -n "Log entry $i - $(date)" | base64)
  RESPONSE=$(curl -s -X POST "$API_URL/api/v1/streams/default/test/logs/events" \
    -H 'Content-Type: application/json' \
    -d "{\"events\": [{\"payload\": \"$PAYLOAD\"}]}")
  if [ $? -eq 0 ]; then
    echo "  ✓ Log entry $i added"
  else
    echo "  ✗ Failed to add log entry $i"
  fi
done

echo ""
echo "Done! Check the Stream Dashboard to see the statistics update."
echo ""
echo "View stats:"
echo "  curl $API_URL/api/v1/streams/default/test/events/stats"
echo "  curl $API_URL/api/v1/streams/default/test/logs/stats"

