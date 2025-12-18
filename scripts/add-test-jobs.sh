#!/bin/bash
# Script to add test jobs to queues for testing the Queue Dashboard

API_URL="${API_URL:-http://localhost:8080}"

echo "Adding test jobs to queues..."
echo ""

# Add jobs to test-queue
echo "Adding 10 jobs to default/test/test-queue..."
for i in {1..10}; do
  PAYLOAD=$(echo -n "Test job $i - $(date)" | base64)
  RESPONSE=$(curl -s -X POST "$API_URL/api/v1/queues/default/test/test-queue/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"payload\": \"$PAYLOAD\"}")
  if [ $? -eq 0 ]; then
    echo "  ✓ Job $i added"
  else
    echo "  ✗ Failed to add job $i"
  fi
done

echo ""
echo "Adding 5 jobs to default/test/priority-queue..."
for i in {1..5}; do
  PAYLOAD=$(echo -n "Priority job $i - $(date)" | base64)
  RESPONSE=$(curl -s -X POST "$API_URL/api/v1/queues/default/test/priority-queue/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"payload\": \"$PAYLOAD\"}")
  if [ $? -eq 0 ]; then
    echo "  ✓ Job $i added"
  else
    echo "  ✗ Failed to add job $i"
  fi
done

echo ""
echo "Done! Check the Queue Dashboard to see the statistics update."
echo ""
echo "View stats:"
echo "  curl $API_URL/api/v1/queues/default/test/test-queue/stats"
echo "  curl $API_URL/api/v1/queues/default/test/priority-queue/stats"

