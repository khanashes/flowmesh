# FlowMesh Go SDK

Go SDK for FlowMesh (coming soon in Phase 28-29).

## Status

🚧 Under Development

This SDK will provide:
- Native Go client for FlowMesh
- gRPC client with connection pooling
- Stream, Queue, and KV APIs
- Context-aware operations

## Future API

```go
package main

import (
    "context"
    "github.com/flowmesh/sdk-go"
)

func main() {
    client := flowmesh.NewClient("localhost:50051")
    ctx := context.Background()
    
    // Streams
    client.Stream.Write(ctx, "orders.events", event)
    for ev := range client.Stream.Consumer(ctx, "orders.events", "billing") {
        process(ev)
    }
    
    // Queues
    client.Queue.Enqueue(ctx, "emails.send", payload)
    client.Queue.Worker(ctx, "emails.send", processJob)
    
    // KV
    client.KV.Set(ctx, "user:234", data, flowmesh.WithTTL(time.Hour))
    value := client.KV.Get(ctx, "user:234")
}
```

See the main [FlowMesh README](../README.md) for project details.

