# FlowMesh Python SDK

Python SDK for FlowMesh (coming soon in Phase 26-27).

## Status

🚧 Under Development

This SDK will provide:
- Python client for FlowMesh
- Async I/O gRPC client
- Stream, Queue, and KV APIs
- Type hints and async patterns

## Future API

```python
import asyncio
from flowmesh import FlowMesh

async def main():
    fm = FlowMesh(endpoint='localhost:50051')
    
    # Streams
    await fm.stream.write('orders.events', event)
    async for ev in fm.stream.consumer('orders.events', 'billing'):
        await process(ev)
    
    # Queues
    await fm.queue.enqueue('emails.send', payload, delay=30)
    await fm.queue.worker('emails.send', process_job)
    
    # KV
    await fm.kv.set('user:234', data, ttl=3600)
    value = await fm.kv.get('user:234')

asyncio.run(main())
```

See the main [FlowMesh README](../README.md) for project details.

