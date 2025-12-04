# FlowMesh Architecture

This document provides a high-level overview of the FlowMesh architecture.

## Overview

FlowMesh is a unified event fabric that combines three core messaging primitives:
- **Key-Value Cache** (Redis replacement)
- **Job Queues** (RabbitMQ/Celery replacement)
- **Event Streams** (Kafka replacement)

All three primitives share a common storage engine and unified APIs.

## High-Level Architecture

```
             SDK
              │
    ┌─────────▼─────────┐
    │   FlowMesh API   │
    │  (gRPC + HTTP)   │
    └─────────┬─────────┘
              │
  ┌───────────▼───────────┐
  │ Unified Message Bus  │
  └───────────┬───────────┘
              │
 ┌─────┬─────┬───────────┐
 │ KV  │ Job │ Streams  │
 │Store│Queue│ Commit Log│
 └─────┴─────┴───────────┘
              │
     ┌────────▼────────┐
     │ Embedded DB /  │
     │ Write-Ahead Log│
     └────────────────┘
```

## Core Components

### 1. API Layer

The API layer provides unified access to all FlowMesh primitives:
- **gRPC API**: High-performance streaming API
- **HTTP REST API**: Simple HTTP endpoints
- Authentication and authorization
- Request routing and validation

### 2. Unified Message Bus

The message bus is the core abstraction that routes messages to the appropriate storage backend:
- Message routing logic
- Request validation
- Multi-tenancy support
- Rate limiting and backpressure

### 3. Storage Primitives

#### Key-Value Store
- Embedded KV database (Pebble/Badger)
- TTL support with automatic expiration
- Multiple data types (String, Hash, Set, Sorted Set)
- Fast in-memory indexes

#### Job Queue
- Durable queue storage
- Append-only log for job history
- In-memory priority heap for ready jobs
- Visibility timeout tracking
- Dead-letter queue support

#### Event Streams
- Append-only commit log
- Partitioned storage
- Offset-based reading
- Consumer group management
- Retention policies

### 4. Storage Engine

The unified storage engine provides:
- **Segmented Logs**: Append-only log files for streams and queues
- **Embedded KV**: Fast key-value storage
- **Metadata Store**: Configuration, schemas, consumer offsets
- **Durability**: Configurable fsync policies

## Data Model

### Resource Addressing

All resources are addressed hierarchically:
```
tenant / namespace / resource_type / resource_name
```

Resource types:
- `kv:tenant:namespace:name` - Key-value store
- `queue:tenant:namespace:name` - Job queue
- `stream:tenant:namespace:name` - Event stream

### Message Structure

All messages share a common structure:
```go
type Message struct {
    ID            string
    ResourcePath  string
    Partition     int32
    Offset        int64
    Seq           int64
    Type          MessageType  // STREAM, QUEUE, KV_EVENT
    Payload       []byte
    Headers       map[string]string
    CreatedAt     time.Time
    VisibleAt     time.Time
    Attempts      int32
    SchemaVersion int32
}
```

## Storage Layout

### Single Node (Phase 1)

```
data/
  streams/
    <hash_of_resource>/segment-000001.log
    <hash_of_resource>/segment-000002.log
  queues/
    <hash_of_resource>/segment-000001.log
  kv/
    store.db
  metadata/
    resources.json
    schemas.json
    consumers.json
```

### Future: Clustered (Phase 2)

- Partitioned storage across nodes
- Raft-based replication
- Leader/follower architecture
- Partition assignment and rebalancing

## Concurrency Model

FlowMesh uses goroutines for concurrent operations:

- **APIServer**: Handles incoming requests
- **StreamManager**: Manages stream operations and consumer groups
- **QueueManager**: Manages queue state and worker coordination
- **KVStore**: Handles key-value operations
- **Schedulers**: Background tasks (TTL cleanup, visibility timeout, etc.)
- **MetricsExporter**: Collects and exports metrics

## Key Features

### 1. Unified Autoscaling

Built-in metrics enable automatic scaling:
- Queue depth and lag
- Message age
- Processing throughput
- Worker health

### 2. Time-Travel Debugging

- Create replay sessions from any offset/timestamp
- Sandbox consumer groups
- Inspect historical messages
- Replay into test environments

### 3. Observability

- OpenTelemetry tracing for all operations
- Prometheus metrics endpoint
- Distributed tracing across operations
- Unified dashboard (coming soon)

### 4. Schema Validation

- JSON Schema validation on write
- Type-safe SDK generation
- Versioned schema migrations
- Backward compatibility checks

## Development Phases

### Phase 1: MVP (Single Node)

Current phase focusing on:
- Single-node implementation
- Core primitives (KV, Queues, Streams)
- Basic APIs
- Local development experience

### Phase 2: Clustering

Future enhancements:
- Multi-node deployment
- Raft consensus
- Partitioning and sharding
- High availability

### Phase 3: Enterprise

Future enterprise features:
- Cloud deployments
- Managed service
- Compliance and security
- Advanced monitoring

## Technology Choices

- **Language**: Go (performance, concurrency, deployment simplicity)
- **Storage**: Pebble/Badger (embedded KV), custom segmented logs
- **API**: gRPC (performance) + HTTP REST (simplicity)
- **Observability**: OpenTelemetry, Prometheus
- **SDKs**: Node.js, Python, Go

## Design Principles

1. **Unified First**: All primitives share common infrastructure
2. **Developer Experience**: Local-first, simple APIs, great tooling
3. **Observability**: Built-in tracing and metrics
4. **Durability**: Configurable persistence guarantees
5. **Extensibility**: Plugin architecture for future features

## Further Reading

- [Engine Level Design](../engine_level_design.md) - Detailed implementation design
- [API Documentation](API.md) - API reference (coming soon)
- [Deployment Guide](DEPLOYMENT.md) - Deployment instructions (coming soon)

