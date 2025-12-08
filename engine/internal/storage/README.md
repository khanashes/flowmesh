# FlowMesh Storage Framework

A beautiful, well-structured storage framework implementing design patterns for managing streams, queues, and key-value stores.

## Architecture

The storage framework follows several design patterns:

### 1. **Builder Pattern** (`builder.go`)
Provides a fluent interface for constructing `Storage` instances with optional configuration:

```go
storage, err := NewBuilder().
    WithDataDir("./data").
    WithConfig(customConfig).
    BuildAndStart(ctx)
```

### 2. **Factory Pattern** (`factory.go`)
Simplifies resource creation (streams, queues, KV stores):

```go
factory := NewResourceFactory(storage.MetaStore())
stream, err := factory.CreateStream(ctx, "tenant1", "ns1", "my-stream", 1)
queue, err := factory.CreateQueue(ctx, "tenant1", "ns1", "my-queue", 1)
```

### 3. **Interface-Based Design** (`interfaces.go`)
All components implement well-defined interfaces:
- `StorageBackend` - Main storage interface
- `StreamManager` - Stream operations
- `QueueManager` - Queue operations
- `Lifecycle` - Component lifecycle management

### 4. **Wrapper Pattern** (`wrappers.go`)
Wraps concrete implementations to provide context support and interface compliance:
- `streamManagerWrapper` - Wraps `streams.Manager`
- `queueManagerWrapper` - Wraps `queues.Manager`

### 5. **Configuration Pattern** (`config.go`)
Centralized configuration with validation:

```go
config := &Config{
    DataDir:        "./data",
    SegmentMaxSize: 100 * 1024 * 1024,
    SegmentMaxAge:  24 * time.Hour,
    FlushInterval:  5 * time.Second,
}
```

## Usage Examples

### Basic Usage

```go
// Simple creation
storage, err := New("./data")
if err != nil {
    log.Fatal(err)
}
defer storage.Stop(context.Background())

// Start the storage system
ctx := context.Background()
if err := storage.Start(ctx); err != nil {
    log.Fatal(err)
}
```

### Using Builder Pattern

```go
storage, err := NewBuilder().
    WithDataDir("./data").
    WithConfig(DefaultConfig()).
    BuildAndStart(ctx)
```

### Creating Resources

```go
factory := NewResourceFactory(storage.MetaStore())

// Create a stream
stream, err := factory.CreateStream(ctx, "tenant1", "ns1", "events", 1)

// Create a queue
queue, err := factory.CreateQueue(ctx, "tenant1", "ns1", "jobs", 1)
```

### Working with Streams

```go
streamMgr := storage.StreamManager()

// Write events
events := []StreamEvent{
    {Payload: []byte("event1"), Headers: map[string]string{"key": "value"}},
}
offsets, err := streamMgr.WriteEvents(ctx, stream.GetPath(), events)

// Read events
messages, err := streamMgr.ReadFromOffset(ctx, stream.GetPath(), 0, 0, 10)
```

### Working with Queues

```go
queueMgr := storage.QueueManager()

// Enqueue a job
jobID, seq, err := queueMgr.Enqueue(ctx, queue.GetPath(), []byte("payload"), QueueEnqueueOptions{
    Delay: 0,
})

// Reserve a job
job, err := queueMgr.Reserve(ctx, queue.GetPath(), 30*time.Second)
if job != nil {
    // Process job...
    queueMgr.RemoveFromInFlight(ctx, queue.GetPath(), job.ID)
}
```

## Design Principles

1. **Separation of Concerns**: Each component has a single responsibility
2. **Dependency Injection**: Components receive dependencies via constructors
3. **Interface Segregation**: Small, focused interfaces
4. **Context Propagation**: All operations support context for cancellation/timeouts
5. **Lifecycle Management**: Components implement `Lifecycle` interface
6. **Error Handling**: Proper error wrapping and custom error types

## Component Structure

```
storage/
├── interfaces.go    # Core interfaces
├── storage.go       # Main Storage implementation
├── builder.go       # Builder pattern
├── factory.go       # Factory pattern
├── config.go        # Configuration
├── wrappers.go      # Interface adapters
└── examples.go      # Usage examples
```

## Benefits

- **Testability**: Interfaces enable easy mocking
- **Extensibility**: New implementations can be added via interfaces
- **Maintainability**: Clear separation of concerns
- **Flexibility**: Builder pattern allows custom configurations
- **Type Safety**: Strong typing throughout
- **Context Support**: Proper cancellation and timeout handling

