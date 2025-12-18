# Testing the Queue Dashboard

The Queue Dashboard requires queues to exist in the MetaStore before they can be viewed. This guide shows you how to create test queues with sample jobs.

## Quick Start

### Option 1: Using the Test Queue Creation Script (Recommended)

Run the provided script to create test queues and add sample jobs:

```bash
cd engine
go run cmd/create-test-queues/main.go
```

This will:
- Create 3 test queues:
  - `default/test/test-queue` (with 5 test jobs)
  - `default/test/priority-queue`
  - `production/orders/order-processing`
- Initialize the queues
- Add 5 test jobs to `default/test/test-queue`

After running this script, refresh your browser at http://localhost:8080/queues and you should see the queues listed.

### Option 2: Using Docker

If you're running FlowMesh in Docker, you can run the script inside the container:

```bash
# Find the container name
docker ps | grep flowmesh

# Copy the script to the container (or mount it as a volume)
# Then execute it
docker exec flowmesh go run /path/to/create-test-queues/main.go
```

Or, if you have access to the container's shell:

```bash
docker exec -it flowmesh sh
cd /app
go run cmd/create-test-queues/main.go
```

### Option 3: Manual Creation via API

You can also create queues manually by using the storage API or by adding an HTTP endpoint to create queues (not currently implemented).

## Adding More Jobs

Once queues exist, you can add more jobs using the HTTP API:

```bash
# Add a job to a queue
curl -X POST http://localhost:8080/api/v1/queues/default/test/test-queue/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload": "SGVsbG8gV29ybGQ="}'  # base64 encoded payload
```

Or using base64 encoding for text:

```bash
# Add a text job
echo -n "My job payload" | base64
# Then use the encoded value in the payload field
curl -X POST http://localhost:8080/api/v1/queues/default/test/test-queue/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload": "TXkgam9iIHBheWxvYWQ="}'
```

## Viewing Queue Stats

View queue statistics via the API:

```bash
curl http://localhost:8080/api/v1/queues/default/test/test-queue/stats
```

Or simply refresh the Web UI at http://localhost:8080/queues - stats update automatically every 5 seconds.

## Troubleshooting

### No queues showing up

1. Make sure the FlowMesh server is running
2. Verify queues were created: Check `./data/metadata/resources.json` for queue entries
3. Check server logs for errors
4. Try refreshing the browser (hard refresh: Ctrl+Shift+R or Cmd+Shift+R)

### Queue stats not updating

1. Check browser console for API errors
2. Verify the queue name is correct (tenant/namespace/name format)
3. Check network tab to see if API calls are succeeding

### Jobs not appearing

1. Make sure jobs were enqueued successfully (check script output)
2. Jobs might be in-flight if they were reserved
3. Check the "In-Flight Jobs" count in the stats

