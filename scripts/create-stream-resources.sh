#!/bin/bash
# Script to create stream resources in MetaStore for testing

CONTAINER_NAME="flowmesh"
METADATA_DIR="/app/data/metadata"
RESOURCES_FILE="${METADATA_DIR}/resources.json"

echo "Creating stream resources in Docker container: ${CONTAINER_NAME}..."

# Ensure the metadata directory exists in the container
docker exec -u root "$CONTAINER_NAME" mkdir -p "$METADATA_DIR"

# Read existing resources if file exists
EXISTING_JSON=$(docker exec "$CONTAINER_NAME" cat "$RESOURCES_FILE" 2>/dev/null || echo "{}")

# Generate the JSON content for resources.json
# We'll merge with existing resources
JSON_CONTENT=$(python3 <<EOF
import json
import sys

# Parse existing resources
try:
    existing = json.loads('''$EXISTING_JSON''')
except:
    existing = {}

# Stream resources to add
streams = {
    "default/test/stream/events": {
        "tenant": "default",
        "namespace": "test",
        "name": "events",
        "type": "stream",
        "partitions": 1,
        "created_at": "2024-12-19T00:00:00Z",
        "updated_at": "2024-12-19T00:00:00Z"
    },
    "default/test/stream/logs": {
        "tenant": "default",
        "namespace": "test",
        "name": "logs",
        "type": "stream",
        "partitions": 1,
        "created_at": "2024-12-19T00:00:00Z",
        "updated_at": "2024-12-19T00:00:00Z"
    },
    "production/metrics/stream/telemetry": {
        "tenant": "production",
        "namespace": "metrics",
        "name": "telemetry",
        "type": "stream",
        "partitions": 1,
        "created_at": "2024-12-19T00:00:00Z",
        "updated_at": "2024-12-19T00:00:00Z"
    }
}

# Merge with existing
existing.update(streams)

# Output JSON
print(json.dumps(existing, indent=2))
EOF
)

# Write the JSON content to the file inside the container
echo "${JSON_CONTENT}" | docker exec -i "$CONTAINER_NAME" sh -c "cat > ${RESOURCES_FILE}"

if [ $? -eq 0 ]; then
  echo "✓ Resources file updated: ${RESOURCES_FILE}"
  echo ""
  echo "Restart the FlowMesh container for changes to take effect:"
  echo "  docker-compose restart flowmesh"
else
  echo "✗ Failed to update resources file."
  exit 1
fi

