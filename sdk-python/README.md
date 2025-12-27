# FlowMesh Python SDK

Python SDK for FlowMesh - A unified async client for Streams, Queues, and KV operations.

## Status

✅ **Ready for use** - Full implementation of Stream, Queue, and KV clients with async/await support

## Installation

```bash
# Install from source
pip install -e .

# Or install dependencies first
pip install -e .[dev]
```

## Requirements

- Python 3.9+
- FlowMesh server running (default: `localhost:50051`)

## Quick Start

### Create a Client

```python
import asyncio
from flowmesh import FlowMesh

async def main():
    # Create client using async context manager
    async with FlowMesh(endpoint="localhost:50051") as client:
        # Use the client
        await client.health_check()

# Or create and manage manually
async def main():
    client = FlowMesh(endpoint="localhost:50051", auth_token="your-token")
    await client.connect()
    try:
        await client.health_check()
    finally:
        await client.close()

asyncio.run(main())
```

### Streams

#### Write Events

```python
from flowmesh import FlowMesh, Event

async with FlowMesh(endpoint="localhost:50051") as client:
    events = [
        Event(
            payload=b'{"order_id": "123", "amount": 99.99}',
            headers={"event_type": "order.created"},
        ),
    ]
    
    offsets = await client.stream.write("tenant", "namespace", "stream-name", events)
    print(f"Events written at offsets: {offsets}")
```

#### Read Messages

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    messages = await client.stream.read(
        "tenant", "namespace", "stream-name",
        partition=0, offset=0, max_messages=10
    )
    
    for msg in messages:
        print(f"Offset: {msg.offset}, Payload: {msg.payload}")
```

#### Subscribe to Stream

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    async for msg in client.stream.subscribe(
        "tenant",
        "namespace",
        "stream-name",
        "consumer-group",
        partition=0,
        start_offset=-2,  # -2 for latest, -1 for committed
    ):
        process_message(msg)
        # Commit offset
        await client.stream.commit_offset(
            "tenant", "namespace", "stream-name",
            "consumer-group", 0, msg.offset
        )
```

### Queues

#### Enqueue Jobs

```python
from datetime import timedelta

async with FlowMesh(endpoint="localhost:50051") as client:
    job_id, seq = await client.queue.enqueue(
        "tenant",
        "namespace",
        "queue-name",
        b'{"task": "process_order"}',
        delay=timedelta(seconds=5),  # Optional delay
        headers={"priority": "high"},
    )
```

#### Reserve and Process Jobs

```python
from datetime import timedelta

async with FlowMesh(endpoint="localhost:50051") as client:
    # Reserve a single job
    job = await client.queue.reserve(
        "tenant",
        "namespace",
        "queue-name",
        visibility_timeout=timedelta(seconds=30),
    )
    
    if job:
        process_job(job)
        # Acknowledge
        await client.queue.ack("tenant", "namespace", "queue-name", job.id)
```

#### Batch Receive

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    jobs = await client.queue.receive(
        "tenant",
        "namespace",
        "queue-name",
        max_jobs=10,
        visibility_timeout=timedelta(seconds=30),
    )
```

#### Worker Pattern

```python
async def worker(client):
    while True:
        job = await client.queue.reserve(
            "tenant",
            "namespace",
            "queue-name",
            visibility_timeout=timedelta(seconds=30),
            long_poll_timeout=timedelta(seconds=5),
        )
        
        if not job:
            continue
        
        try:
            await process_job(job)
            await client.queue.ack("tenant", "namespace", "queue-name", job.id)
        except Exception:
            # NACK on failure
            await client.queue.nack("tenant", "namespace", "queue-name", job.id)

async with FlowMesh(endpoint="localhost:50051") as client:
    await worker(client)
```

#### Queue Statistics

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    stats = await client.queue.get_stats("tenant", "namespace", "queue-name")
    print(f"Pending: {stats.pending_jobs}, InFlight: {stats.in_flight_jobs}")
```

### KV Store

#### Set and Get

```python
from datetime import timedelta

async with FlowMesh(endpoint="localhost:50051") as client:
    # Set with TTL
    await client.kv.set(
        "tenant",
        "namespace",
        "store-name",
        "key",
        b'{"value": "data"}',
        ttl=timedelta(hours=1),
    )
    
    # Get
    value = await client.kv.get("tenant", "namespace", "store-name", "key")
```

#### List Keys

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    # List all keys
    keys = await client.kv.list_keys("tenant", "namespace", "store-name", prefix="")
    
    # List keys with prefix
    user_keys = await client.kv.list_keys(
        "tenant", "namespace", "store-name", prefix="user:"
    )
```

#### Delete and Check Existence

```python
async with FlowMesh(endpoint="localhost:50051") as client:
    # Check if key exists
    exists = await client.kv.exists("tenant", "namespace", "store-name", "key")
    
    # Delete
    await client.kv.delete("tenant", "namespace", "store-name", "key")
```

## API Reference

### FlowMesh Client

- `FlowMesh(endpoint: str, auth_token: Optional[str] = None)` - Create client
- `async connect()` - Connect to server
- `async close()` - Close connection
- `async health_check() -> bool` - Health check
- `async readiness_check() -> bool` - Readiness check

### Stream Client

- `write(tenant, namespace, stream, events) -> List[int]` - Write events
- `read(tenant, namespace, stream, partition=0, offset=0, max_messages=10) -> List[Message]` - Read messages
- `subscribe(tenant, namespace, stream, consumer_group, partition=0, start_offset=-1) -> AsyncIterator[Message]` - Subscribe (async generator)
- `commit_offset(...)` - Commit offset
- `get_offset(...) -> int` - Get committed offset
- `get_latest_offset(...) -> int` - Get latest offset
- `get_consumer_group_state(...) -> ConsumerGroupState` - Get consumer group state

### Queue Client

- `enqueue(tenant, namespace, queue, payload, delay=None, headers=None) -> tuple[str, int]` - Enqueue job
- `reserve(tenant, namespace, queue, visibility_timeout=None, long_poll_timeout=None) -> Optional[Job]` - Reserve job
- `receive(tenant, namespace, queue, max_jobs=1, visibility_timeout=None, long_poll_timeout=None) -> List[Job]` - Batch receive
- `ack(tenant, namespace, queue, job_id)` - Acknowledge job
- `nack(tenant, namespace, queue, job_id, delay=None)` - Negative acknowledge
- `get_stats(tenant, namespace, queue) -> QueueStats` - Get queue statistics
- `set_retry_policy(...)` - Set retry policy
- `get_retry_policy(...) -> RetryPolicy` - Get retry policy
- `list_dlq_jobs(...) -> List[Job]` - List DLQ jobs
- `replay_dlq_job(...) -> tuple[str, int]` - Replay DLQ job

### KV Client

- `set(tenant, namespace, store, key, value, ttl=None)` - Set key-value
- `get(tenant, namespace, store, key) -> bytes` - Get value
- `delete(tenant, namespace, store, key)` - Delete key
- `exists(tenant, namespace, store, key) -> bool` - Check existence
- `list_keys(tenant, namespace, store, prefix="") -> List[str]` - List keys

## Error Handling

The SDK raises custom exceptions that inherit from `FlowMeshError`:

```python
from flowmesh import FlowMesh, FlowMeshError, FlowMeshNotFoundError

try:
    await client.stream.read(...)
except FlowMeshNotFoundError:
    # Handle not found
except FlowMeshError as e:
    # Handle other errors
```

## Examples

See the [examples directory](./examples/) for complete working examples:

- [Streams Example](./examples/streams_example.py) - Writing, reading, and subscribing to streams
- [Queues Example](./examples/queues_example.py) - Enqueueing, reserving, and processing jobs
- [KV Example](./examples/kv_example.py) - Setting, getting, and listing keys

## Development

### Generate Protocol Buffers

```bash
./generate_proto.sh
```

### Run Tests

```bash
pytest
```

### Type Checking

```bash
mypy flowmesh/
```

### Format Code

```bash
black flowmesh/ examples/ tests/
ruff check flowmesh/ examples/ tests/
```

## License

Apache License 2.0 - See the main [FlowMesh README](../README.md) for details.
