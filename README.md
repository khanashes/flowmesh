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

### ✅ Unified Platform
- **Single System**: KV Store, Job Queues, and Event Streams in one platform
- **Shared Infrastructure**: Common storage, APIs, metrics, and observability
- **Production Ready**: Full implementation with durability, recovery, and error handling

### ✅ Time-Travel Debugging
- Rewind to any offset or timestamp
- Replay events into sandbox workers
- Inspect payloads & side effects
- Full debugging visibility via Web UI

### ✅ Built-in Observability
- OpenTelemetry tracing automatically
- Distributed tracing across all operations
- Prometheus metrics endpoint
- Unified Web UI dashboard

### ✅ Schema Validation Layer
- JSON Schema validation on write
- Schema versioning and migration support
- Type-safe SDKs (Go ✅, Python ✅)

### ✅ Developer Experience
- Web UI for monitoring and debugging (Queue, Stream, Replay dashboards)
- Multiple SDKs (Go ✅, Python ✅)
- Comprehensive documentation
- Docker support for easy deployment
- Local-first development

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

### Using Docker

The Docker image includes both the FlowMesh engine and the Web UI:

```bash
# Build Docker image (builds both Go binary and Web UI)
make docker-build

# Run with Docker Compose (production)
make docker-run

# Run in development mode
make docker-dev

# View logs
make docker-logs

# Stop containers
make docker-stop
```

Once running, access:
- **API**: `http://localhost:8080/api/v1/...`
- **Web UI**: `http://localhost:8080` (served automatically)
- **gRPC**: `localhost:50051`
- **Metrics**: `http://localhost:9090/metrics`

For more Docker details, see [docker/README.md](docker/README.md).

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
- **SDKs**: Go SDK ✅, Python SDK ✅, Node.js SDK (planned)
- **Web UI**: React-based dashboard for monitoring and debugging

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 📦 Project Structure

```
flowmesh/
├── engine/              # Core Go server
├── sdk-node/           # Node.js SDK (planned)
├── sdk-python/         # Python SDK ✅
├── sdk-go/             # Go SDK ✅
├── web-ui/             # React dashboard
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

### Phase 1 – MVP (✅ Complete)
- ✅ Single-node engine
- ✅ Basic project structure
- ✅ Streams + Queues + KV (fully implemented)
- ✅ SDKs (Go ✅, Python ✅, Node.js planned)
- ✅ Web UI (Queue, Stream, Replay dashboards)
- ✅ Replay debugger with full UI

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

**Status**: ✅ MVP Complete - Production Ready (v0.1.0)

For questions, issues, or discussions, please open an issue on GitHub.

