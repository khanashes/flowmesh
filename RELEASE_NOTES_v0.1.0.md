# FlowMesh v0.1.0 Release Notes

**Release Date**: December 28, 2025

## 🎉 Major Milestone: MVP Complete!

FlowMesh v0.1.0 represents the completion of the MVP (Minimum Viable Product) with a fully functional unified event fabric platform. This release includes all core features for single-node deployment with production-ready APIs, SDKs, and Web UI.

## ✨ What's New

### Complete Platform Implementation

FlowMesh now provides a unified platform for all three core messaging primitives:

1. **Key-Value Store** - Redis-like caching with TTL support
2. **Job Queues** - Priority queues with worker coordination (SQS/RabbitMQ-like)
3. **Event Streams** - Log-based streaming with consumer groups (Kafka-like)

### Web UI Dashboard

A fully-featured React dashboard provides:
- **Queue Dashboard**: Monitor queue depth, job statistics, and processing status
- **Stream Dashboard**: View streams, consumer groups, and lag metrics
- **Replay Interface**: Create and manage replay sessions for debugging
- Real-time updates via polling (5-second intervals)
- Responsive design with dark mode support

### SDKs Available

Two production-ready SDKs are now available:

#### Go SDK ✅
- Idiomatic Go client with type safety
- Connection pooling and error handling
- Functional options pattern
- Comprehensive examples and tests
- Install: `go get github.com/flowmesh/sdk-go`

#### Python SDK ✅
- Full async/await support
- Type hints throughout
- Async generators for stream subscription
- Comprehensive examples and tests
- Install: `pip install -e .` (from sdk-python directory)

### Core Features

- **Time-Travel Replay**: Debug production issues by replaying events from any offset or timestamp
- **Schema Validation**: JSON Schema validation on write with versioning
- **Observability**: Built-in OpenTelemetry tracing and Prometheus metrics
- **Durability**: Configurable fsync policies and checkpoint-based crash recovery
- **Advanced Querying**: Custom filter expression language for streams, queues, and KV stores

## 📊 Statistics

- **Phases Completed**: 26/26 core phases (100% MVP completion)
- **API Endpoints**: 50+ gRPC and HTTP endpoints
- **Test Coverage**: Comprehensive unit and integration tests
- **Documentation**: Complete READMEs, architecture docs, and examples

## 🚀 Quick Start

```bash
# Start FlowMesh server
docker-compose up

# Or build and run locally
cd engine
make build
./bin/flowmesh

# Access Web UI at http://localhost:5173
```

## 📚 Documentation

- [Main README](README.md) - Overview and quick start
- [Architecture Guide](docs/ARCHITECTURE.md) - System design and components
- [Go SDK README](sdk-go/README.md) - Go SDK documentation
- [Python SDK README](sdk-python/README.md) - Python SDK documentation
- [Contributing Guide](CONTRIBUTING.md) - Development guidelines

## 🎯 What's Next

- **Phase 23**: Multi-node clustering with Raft consensus
- **Node.js SDK**: JavaScript/TypeScript client library
- **WebSocket Support**: Real-time UI updates
- **Cloud Deployment**: Kubernetes operators and Helm charts

## 🙏 Acknowledgments

Thank you to all contributors and early adopters who helped shape FlowMesh into a production-ready platform.

---

**Upgrade Notes**: This is the initial release (v0.1.0). No migration required.

**Support**: For issues and questions, please open an issue on GitHub.

