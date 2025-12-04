# FlowMesh – Unified Event Fabric

**One platform for Cache + Job Queues + Event Streams**

FlowMesh unifies all three core messaging primitives (key-value caching, job queues, and event streaming) under a single clustered platform, eliminating the need to deploy and operate multiple separate systems.

## 🧠 Problem Statement

Modern backend systems must handle three core messaging primitives:
1. **Key-value caching** → Redis
2. **Job/work queues** → RabbitMQ / SQS / Celery
3. **Event streaming** → Kafka / Pulsar / Kinesis

Currently, teams deploy and operate 3–5 separate systems, each with its own:
- Client SDK
- Metrics format
- Authentication
- Deployment model
- Scaling logic
- Observability toolset

This leads to high operational overhead, cognitive load on developers, duplicated infrastructure expenses, and poor debugging visibility across system boundaries.

## ✅ Solution

FlowMesh unifies all three primitives under a single platform:

```
┌──────────────────────────────┐
│         FlowMesh             │
│                              │
│   CACHE   JOB QUEUE   STREAM │
│   (KV)    (Workers)   (Logs) │
│                              │
│      Shared Storage Engine  │
│      Shared SDK              │
│      Shared Metrics          │
│      Shared UI Dashboard     │
└──────────────────────────────┘
```

Instead of: **Redis + RabbitMQ + Kafka + custom workers + tracing hacks + scaling glue**

Developers get: **FlowMesh**

## 🌟 Key Features

### Unified Autoscaling & Backpressure
- Built-in metrics for queue depth, message lag, processing time
- Native Kubernetes scaling output
- Automatic worker scaling based on load

### Time-Travel Debugging
- Rewind to any offset or timestamp
- Replay events into sandbox workers
- Inspect payloads & side effects
- Full debugging visibility

### Built-in Observability
- OpenTelemetry tracing automatically
- Distributed tracing across all operations
- Prometheus metrics endpoint
- Unified observability dashboard

### Schema Validation Layer
- JSON Schema validation on write
- SDK type generation
- Versioned schema migrations

### Local-First DX
- `flowmesh dev` - Start local development environment
- In-memory cluster mode
- Web UI included
- Same config for local and production

## 🚀 Quick Start

### Prerequisites

- Go 1.21 or later
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/flowmesh/flowmesh.git
cd flowmesh

# Build the engine
cd engine
make build

# Run FlowMesh
./bin/flowmesh
```

### Using the Binary

```bash
# Run with defaults (gRPC on :50051, HTTP on :8080)
./bin/flowmesh

# Run with custom configuration
./bin/flowmesh \
  --grpc-addr :50051 \
  --http-addr :8080 \
  --data-dir ./data \
  --log-level info
```

### Environment Variables

```bash
export FLOWMESH_GRPC_ADDR=:50051
export FLOWMESH_HTTP_ADDR=:8080
export FLOWMESH_DATA_DIR=./data
export FLOWMESH_LOG_LEVEL=info
./bin/flowmesh
```

## 🏗️ Architecture

FlowMesh is built as a single-node Go service (Phase 1) with:
- **Storage Engine**: Append-only segmented logs + embedded KV store
- **API Layer**: gRPC + HTTP REST APIs
- **SDKs**: Node.js, Python, Go (coming soon)
- **Web UI**: React-based dashboard for monitoring and debugging (coming soon)

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 📦 Project Structure

```
flowmesh/
├── engine/              # Core Go server
├── sdk-node/           # Node.js SDK (coming soon)
├── sdk-python/         # Python SDK (coming soon)
├── sdk-go/             # Go SDK (coming soon)
├── web-ui/             # React dashboard (coming soon)
├── examples/           # Example applications
├── docs/               # Documentation
└── helm/               # Kubernetes deployment (coming soon)
```

## 🛠️ Development

### Setup Development Environment

```bash
# Run the development setup script
./scripts/dev-setup.sh

# Verify your environment
./scripts/verify.sh
```

### Build

```bash
make build
```

### Test

```bash
make test
```

### Lint

```bash
make lint
```

### Run Locally

```bash
make dev
```

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).

## 📚 Documentation

- [Architecture Overview](docs/ARCHITECTURE.md)
- [Development Guide](CONTRIBUTING.md)
- [API Documentation](docs/API.md) (coming soon)
- [Deployment Guide](docs/DEPLOYMENT.md) (coming soon)

## 🗺️ Roadmap

### Phase 1 – MVP (Current)
- ✅ Single-node engine
- ✅ Basic project structure
- 🔄 Streams + Queues + KV (in progress)
- 🔄 SDKs (Node + Python)
- 🔄 Web UI
- 🔄 Replay debugger

### Phase 2 – Clustering (Future)
- Raft leader election
- Partitioning
- Sharded logs
- Multi-node consumers

### Phase 3 – Enterprise (Future)
- BYOC cloud deployments
- Cloud hosted service
- SOC2 compliance

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

FlowMesh is designed to solve the real-world problem of infrastructure sprawl in modern distributed systems, inspired by the operational challenges teams face with Redis, RabbitMQ, and Kafka.

---

**Status**: 🚧 Early Development - Phase 1 MVP in progress

For questions, issues, or discussions, please open an issue on GitHub.

