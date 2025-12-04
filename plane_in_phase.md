This document outlines all phases of the FlowMesh Unified Event Fabric project, broken down into small, incremental phases. Each phase will have its own detailed implementation plan.
Phase Overview
Phase 1: Project Foundation & Setup
Repository structure setup
Development environment configuration
Basic Go project scaffolding
CI/CD basics
Phase 2: Core Storage Engine Foundation
MetaStore implementation
LogManager with segmented log files
Basic resource metadata management
Storage directory structure
Phase 3: Streams Core Implementation
Single partition stream storage
Write path with offset assignment
Read path with offset-based retrieval
Basic offset index management
Phase 4: Queue Core Implementation
QueueManager foundation
Enqueue operation with log append
ReadyHeap for pending jobs
InFlight job tracking
Phase 5: Queue Worker Operations
Reserve/Receive operations
ACK and NACK handling
Visibility timeout scheduler
Basic retry mechanism
Phase 6: KV Store Implementation
Embedded DB integration (Pebble/Badger)
Set/Get operations
TTL support with expiry index
Background TTL reaper
Phase 7: Consumer Groups & Offset Management
Consumer group metadata
Offset commit and tracking
Consumer group state management
Lag calculation
Phase 8: API Server Foundation
gRPC server setup
HTTP/REST API setup
Authentication/authorization basics
Request routing infrastructure
Phase 9: Streams API Endpoints
WriteEvents gRPC endpoint
ReadStream/Subscribe endpoints
Consumer group subscription
Offset commit endpoints
Phase 10: Queues API Endpoints
Enqueue endpoint
Reserve/Receive endpoint
ACK/NACK endpoints
Queue status endpoints
Phase 11: KV API Endpoints
Set/Get endpoints
Delete operations
TTL management endpoints
Key listing/scanning
Phase 12: Dead Letter Queue & Advanced Retry
DLQ implementation
Configurable retry policies
Exponential backoff
Max attempts handling
Phase 13: Schema Validation
JSON Schema integration
Schema registry
Validation on write
Schema versioning basics
Phase 14: Metrics & Observability
Prometheus metrics exporter
Queue metrics (depth, lag, age)
Stream metrics (throughput, lag)
Node-level metrics
Phase 15: OpenTelemetry Tracing
Tracing integration
Span creation for operations
Trace context propagation
Export to OTLP
Phase 16: Time-Travel Replay Core
Replay session creation
Offset/timestamp lookup
Sandbox consumer groups
Replay state management
Phase 17: Crash Recovery & Durability
Fsync policies
Checkpoint mechanism
Recovery on startup
Queue state reconstruction
Phase 18: Web UI Foundation
React + Vite setup
Basic dashboard layout
WebSocket connection setup
API client integration
Phase 19: Web UI - Queue Dashboard
Queue list view
Queue depth visualization
Worker health display
Job status monitoring
Phase 20: Web UI - Stream Dashboard
Stream/topic list
Offset visualization
Consumer group status
Lag metrics display
Phase 21: Web UI - Replay Interface
Replay session creation UI
Offset/timestamp picker
Replay controls (start/pause/stop)
Replay results display
Phase 22: Node.js SDK - Core
gRPC client setup
Basic connection management
Error handling
TypeScript types
Phase 23: Node.js SDK - Streams API
Stream write methods
Consumer subscription with async iterators
Offset commit utilities
Phase 24: Node.js SDK - Queues API
Enqueue methods
Worker registration
Job processing utilities
Phase 25: Node.js SDK - KV API
Set/Get methods
TTL configuration
Helper utilities
Phase 26: Python SDK - Core
gRPC client setup
Async I/O support
Connection management
Error handling
Phase 27: Python SDK - Full API
Streams API methods
Queues API methods
KV API methods
Async consumer patterns
Phase 28: Go SDK - Core
Native gRPC client
Connection pooling
Error handling
Phase 29: Go SDK - Full API
Streams API
Queues API
KV API
Consumer patterns
Phase 30: Local Development Mode
flowmesh dev command
In-memory mode option
Local persistence
Debug mode features
Phase 31: Docker & Containerization
Dockerfile for engine
Docker Compose setup
Multi-stage builds
Health checks
Phase 32: Documentation & Examples
README and getting started
API documentation
Example applications
Deployment guides
Each phase builds incrementally on previous phases, allowing for continuous validation and testing throughout development.