#!/bin/bash
# Script to create test queues directly in Docker container by writing resources.json

docker exec flowmesh sh << 'EOF'
# Create resources.json with test queues (format is a JSON object, not nested)
cat > /app/data/metadata/resources.json << 'RESOURCES'
{
  "default/test/queue/test-queue": {
    "tenant": "default",
    "namespace": "test",
    "name": "test-queue",
    "type": "queue",
    "partitions": 1,
    "created_at": "2024-12-19T00:00:00Z",
    "updated_at": "2024-12-19T00:00:00Z"
  },
  "default/test/queue/priority-queue": {
    "tenant": "default",
    "namespace": "test",
    "name": "priority-queue",
    "type": "queue",
    "partitions": 1,
    "created_at": "2024-12-19T00:00:00Z",
    "updated_at": "2024-12-19T00:00:00Z"
  },
  "production/orders/queue/order-processing": {
    "tenant": "production",
    "namespace": "orders",
    "name": "order-processing",
    "type": "queue",
    "partitions": 1,
    "created_at": "2024-12-19T00:00:00Z",
    "updated_at": "2024-12-19T00:00:00Z"
  }
}
RESOURCES

echo "Queues created in resources.json"
EOF

echo "Done! Queues should now be visible in the Web UI."

