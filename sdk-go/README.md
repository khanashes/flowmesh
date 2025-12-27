# FlowMesh Go SDK

Go SDK for FlowMesh - A unified client for Streams, Queues, and KV operations.

## Status

✅ **Ready for use** - Full implementation of Stream, Queue, and KV clients

## Installation

```bash
go get github.com/flowmesh/sdk-go
```

## Quick Start

### Create a Client

```go
import (
    "context"
    "github.com/flowmesh/sdk-go"
)

// Create a client
client, err := flowmesh.NewClient("localhost:50051")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Optional: Set authentication token
client, err := flowmesh.NewClient(
    "localhost:50051",
    flowmesh.WithAuthToken("your-api-token"),
)
```

### Streams

#### Write Events

```go
    ctx := context.Background()
    
events := []flowmesh.Event{
    {
        Payload: []byte(`{"order_id": "123", "amount": 99.99}`),
        Headers: map[string]string{"event_type": "order.created"},
    },
}

offsets, err := client.Stream.Write(ctx, "tenant", "namespace", "stream-name", events)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Events written at offsets: %v\n", offsets)
```

#### Read Messages

```go
messages, err := client.Stream.Read(ctx, "tenant", "namespace", "stream-name", 0, 0, 10)
if err != nil {
    log.Fatal(err)
}

for _, msg := range messages {
    fmt.Printf("Offset: %d, Payload: %s\n", msg.Offset, string(msg.Payload))
}
```

#### Subscribe to Stream

```go
msgChan, errChan := client.Stream.Subscribe(
    ctx,
    "tenant",
    "namespace",
    "stream-name",
    "consumer-group",
    flowmesh.WithStartOffset(-2), // -2 for latest, -1 for committed
)

go func() {
    for msg := range msgChan {
        processMessage(msg)
        // Commit offset
        client.Stream.CommitOffset(ctx, "tenant", "namespace", "stream-name", "consumer-group", 0, msg.Offset)
    }
}()

go func() {
    for err := range errChan {
        log.Printf("Error: %v", err)
    }
}()
```

### Queues

#### Enqueue Jobs

```go
jobID, seq, err := client.Queue.Enqueue(
    ctx,
    "tenant",
    "namespace",
    "queue-name",
    []byte(`{"task": "process_order"}`),
    flowmesh.WithDelay(5*time.Second), // Optional delay
    flowmesh.WithHeaders(map[string]string{"priority": "high"}),
)
```

#### Reserve and Process Jobs

```go
// Reserve a single job
job, err := client.Queue.Reserve(
    ctx,
    "tenant",
    "namespace",
    "queue-name",
    flowmesh.WithVisibilityTimeout(30*time.Second),
)
if err != nil {
    log.Fatal(err)
}

if job != nil {
    processJob(job)
    // Acknowledge
    client.Queue.ACK(ctx, "tenant", "namespace", "queue-name", job.ID)
}
```

#### Batch Receive

```go
jobs, err := client.Queue.Receive(
    ctx,
    "tenant",
    "namespace",
    "queue-name",
    flowmesh.WithMaxJobs(10),
    flowmesh.WithVisibilityTimeout(30*time.Second),
)
```

#### Worker Pattern

```go
for {
    job, err := client.Queue.Reserve(
        ctx,
        "tenant",
        "namespace",
        "queue-name",
        flowmesh.WithVisibilityTimeout(30*time.Second),
        flowmesh.WithLongPollTimeout(5*time.Second),
    )
    if err != nil {
        log.Printf("Error: %v", err)
        continue
    }

    if job == nil {
        continue // No jobs available
    }

    if err := processJob(job); err != nil {
        // NACK on failure
        client.Queue.NACK(ctx, "tenant", "namespace", "queue-name", job.ID)
    } else {
        // ACK on success
        client.Queue.ACK(ctx, "tenant", "namespace", "queue-name", job.ID)
    }
}
```

#### Queue Statistics

```go
stats, err := client.Queue.GetStats(ctx, "tenant", "namespace", "queue-name")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Pending: %d, InFlight: %d\n", stats.PendingJobs, stats.InFlightJobs)
```

### KV Store

#### Set and Get

```go
// Set with TTL
err := client.KV.Set(
    ctx,
    "tenant",
    "namespace",
    "store-name",
    "key",
    []byte(`{"value": "data"}`),
    flowmesh.WithTTL(1*time.Hour),
)

// Get
value, err := client.KV.Get(ctx, "tenant", "namespace", "store-name", "key")
```

#### List Keys

```go
// List all keys
keys, err := client.KV.ListKeys(ctx, "tenant", "namespace", "store-name", "")

// List keys with prefix
userKeys, err := client.KV.ListKeys(ctx, "tenant", "namespace", "store-name", "user:")
```

#### Delete and Check Existence

```go
// Check if key exists
exists, err := client.KV.Exists(ctx, "tenant", "namespace", "store-name", "key")

// Delete
err := client.KV.Delete(ctx, "tenant", "namespace", "store-name", "key")
```

## API Reference

### Client Options

- `WithAuthToken(token string)` - Set authentication token

### Stream Options

- `WithPartition(partition int32)` - Set partition ID for subscribe
- `WithStartOffset(offset int64)` - Set start offset (-2 for latest, -1 for committed)

### Queue Options

- `WithDelay(delay time.Duration)` - Delay before job becomes visible
- `WithHeaders(headers map[string]string)` - Set job headers
- `WithVisibilityTimeout(timeout time.Duration)` - Set visibility timeout
- `WithLongPollTimeout(timeout time.Duration)` - Set long poll timeout
- `WithMaxJobs(maxJobs int32)` - Maximum number of jobs to receive (1-100)
- `WithNACKDelay(delay time.Duration)` - Delay before requeue on NACK

### KV Options

- `WithTTL(ttl time.Duration)` - Set TTL for key-value pair

## Error Handling

The SDK returns typed errors that implement the standard `error` interface. Errors wrap gRPC status codes:

```go
err := client.Stream.Write(...)
if err != nil {
    if sdkErr, ok := err.(*flowmesh.Error); ok {
        if sdkErr.IsNotFound() {
            // Handle not found
        } else if sdkErr.IsUnauthenticated() {
            // Handle auth error
        }
    }
}
```

## Examples

See the [examples directory](./examples/) for complete working examples:

- [Streams Example](./examples/streams/main.go) - Writing, reading, and subscribing to streams
- [Queues Example](./examples/queues/main.go) - Enqueueing, reserving, and processing jobs
- [KV Example](./examples/kv/main.go) - Setting, getting, and listing keys

## Context Support

All operations accept `context.Context` for cancellation, timeouts, and tracing:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

messages, err := client.Stream.Read(ctx, ...)
```

## Requirements

- Go 1.24.0 or later
- FlowMesh server running (default: `localhost:50051`)

## License

Apache License 2.0 - See the main [FlowMesh README](../README.md) for details.
