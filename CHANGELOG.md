# Changelog

All notable changes to FlowMesh will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Node.js SDK
- Multi-node clustering
- Partition replication (Raft/Paxos)
- Leader election

## [0.2.0] - 2025-12-28

### Added
- **WebSocket Support**: Real-time updates for Web UI with topic-based subscriptions
- **WebSocket Hub**: Backend hub for managing client connections and broadcasting updates
- **Real-time Stats**: Stream and queue statistics update automatically via WebSocket
- **KV Dashboard UI**: Complete UI for KV store management (list, view, set, get, delete, TTL)
- **Replay Session Updates**: Real-time replay session status and progress updates
- **WebSocket Connection Status**: Visual indicator in UI header showing connection status
- **Enhanced Auth**: Token validation in gRPC interceptors with AuthContext
- **Tracing Configuration**: Configurable sampling strategies (always, probabilistic, rate-based)
- **Checkpoint Recovery**: Full state collection from all storage managers for system recovery
- **API Documentation**: Comprehensive API reference for all gRPC and HTTP endpoints
- **Deployment Guide**: Complete production deployment documentation

### Improved
- Frontend hooks now use WebSocket with automatic polling fallback
- Better error handling and connection management in Web UI
- Improved auth interceptor with proper health check skipping

### Fixed
- Test fixes for replay handler stream path format
- WebSocket handler error handling improvements

## [0.1.0] - 2025-12-28

### Added

#### Core Engine
- Complete unified storage engine supporting Streams, Queues, and KV Store
- MetaStore for resource management with hierarchical paths
- Segmented log manager with offset-based retrieval
- Timestamp-to-offset indexing for time-travel queries
- Queue manager with priority queues and ready heap
- In-flight job tracking with visibility timeout
- Dead Letter Queue (DLQ) with configurable retry policies
- Exponential backoff and retry strategies
- Consumer group management with offset tracking and lag calculation
- JSON Schema validation with versioning
- Embedded KV store (Pebble) with TTL support
- Background TTL reaper for automatic key expiration
- Crash recovery with checkpoint mechanism
- Fsync policies (Always/Interval) for durability guarantees
- Custom Filter Expression Language for advanced querying
- Stream filtering with predicate evaluation
- Queue inspection with filter support
- KV store scanning with predicates

#### API Layer
- Full gRPC API server with all service endpoints
- Complete HTTP/REST API with JSON responses
- Authentication and authorization with token-based auth
- Health and readiness check endpoints
- Request routing infrastructure
- Error handling and status code mapping
- OpenTelemetry tracing integration
- Prometheus metrics exporter
- Distributed trace context propagation

#### Web UI
- Modern React + Vite dashboard
- Queue Dashboard with:
  - Queue list view
  - Queue depth visualization
  - Statistics display (total, pending, in-flight, completed, failed)
  - Job status breakdown
  - Real-time polling every 5 seconds
- Stream Dashboard with:
  - Stream/topic list
  - Stream statistics (latest offset)
  - Consumer group status
  - Lag metrics display
  - Real-time polling every 5 seconds
- Replay Interface with:
  - Session creation form
  - Stream selector
  - Offset/timestamp pickers
  - Session list view
  - Detailed session view with progress tracking
  - Replay controls (start/pause/resume/stop/delete)
  - Real-time polling every 5 seconds
- Responsive layout with dark mode support
- API client integration

#### SDKs
- **Go SDK** - Complete idiomatic Go client
  - Stream, Queue, and KV client implementations
  - Connection management with connection pooling
  - Functional options pattern for configuration
  - Error wrapping with typed errors
  - Type-safe resource path handling
  - Comprehensive test coverage
  - Examples for all operations
- **Python SDK** - Full async/await client
  - Async gRPC client with context management
  - Stream, Queue, and KV client implementations
  - Type hints throughout
  - Async generators for stream subscription
  - Dataclasses for type definitions
  - Comprehensive test coverage
  - Examples for all operations

#### Developer Experience
- Complete documentation (README, ARCHITECTURE, CONTRIBUTING)
- Docker support with docker-compose
- Development scripts for testing and setup
- Makefile with common development tasks
- CI/CD pipeline
- Test scripts for API validation
- Example applications

### Technical Details

- **Storage**: Embedded Pebble KV store, custom segmented logs
- **APIs**: gRPC (performance) + HTTP/REST (simplicity)
- **Observability**: OpenTelemetry tracing, Prometheus metrics
- **Languages**: Go (engine), TypeScript/React (UI), Go/Python (SDKs)
- **Requirements**: Go 1.24+, Node.js 20.19+ or 22.12+, Python 3.9+

### Features

- **Unified Platform**: Single system for KV, Queues, and Streams
- **Time-Travel Replay**: Debug by replaying events from any offset/timestamp
- **Schema Validation**: JSON Schema validation on write with versioning
- **Built-in Observability**: OpenTelemetry tracing and Prometheus metrics
- **Local-First**: Easy local development with included Web UI
- **Production Ready**: Durability guarantees, crash recovery, and error handling

### Breaking Changes

None - This is the initial release.

### Known Limitations

- Single-node only (clustering planned for future release)
- Node.js SDK not yet implemented (planned for future release)
- WebSocket support for real-time UI updates deferred

[Unreleased]: https://github.com/flowmesh/flowmesh/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/flowmesh/flowmesh/releases/tag/v0.1.0
