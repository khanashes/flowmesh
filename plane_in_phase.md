# FlowMesh Project Roadmap

This document outlines the development phases of the FlowMesh Unified Event Fabric.

## Phase 1: Project Foundation & Setup [COMPLETED]
- [x] Repository structure setup
- [x] Development environment configuration
- [x] Basic Go project scaffolding
- [x] CI/CD basics

## Phase 2: Core Storage Engine Foundation [COMPLETED]
- [x] MetaStore implementation (Resource management)
- [x] LogManager with segmented log files
- [x] Storage directory structure
- [x] File-based persistence

## Phase 3: Streams Core Implementation [COMPLETED]
- [x] Single partition stream storage
- [x] Write path with offset assignment
- [x] Read path with offset-based retrieval
- [x] Timestamp-to-offset indexing

## Phase 4: Queue Core Implementation [COMPLETED]
- [x] QueueManager foundation
- [x] Enqueue operation with log append
- [x] ReadyHeap for pending jobs (Priority Queue)
- [x] InFlight job tracking

## Phase 5: Queue Worker Operations [COMPLETED]
- [x] Reserve/Receive operations
- [x] ACK and NACK handling
- [x] Visibility timeout management
- [x] Job lifecycle management

## Phase 6: KV Store Implementation [COMPLETED]
- [x] Embedded DB integration (Pebble)
- [x] Set/Get/Delete operations
- [x] TTL support with expiry index
- [x] Background TTL reaper

## Phase 7: Consumer Groups & Offset Management [COMPLETED]
- [x] Consumer group metadata
- [x] Offset commit and tracking
- [x] Consumer group state management
- [x] Lag calculation

## Phase 8: API Server Foundation [PENDING]
- [ ] gRPC server setup
- [ ] HTTP/REST API setup
- [ ] Authentication/authorization basics
- [ ] Request routing infrastructure
> **Note**: Originally planned here, but deferred to allow for more advanced core features first (Replay, Recovery, Advanced Querying).

## Phase 9: Streams API Endpoints [PENDING]
- [ ] WriteEvents gRPC endpoint
- [ ] ReadStream/Subscribe endpoints
- [ ] Consumer group subscription

## Phase 10: Queues API Endpoints [PENDING]
- [ ] Enqueue endpoint
- [ ] Reserve/Receive endpoint
- [ ] ACK/NACK endpoints

## Phase 11: KV API Endpoints [PENDING]
- [ ] Set/Get endpoints
- [ ] Delete operations
- [ ] Key scanning

## Phase 12: Dead Letter Queue & Advanced Retry [COMPLETED]
- [x] DLQ implementation
- [x] Configurable retry policies
- [x] Exponential backoff
- [x] Max attempts handling
> **Note**: Implemented early as part of Queue Core & Worker Operations.

## Phase 13: Schema Validation [COMPLETED]
- [x] JSON Schema integration
- [x] Schema registry
- [x] Validation on write
- [x] Schema versioning basics
> **Note**: Implemented as part of standard resource management.

## Phase 14: Metrics & Observability [COMPLETED]
- [x] Prometheus metrics exporter
- [x] Queue metrics (depth, lag)
- [x] Stream metrics (throughput)
- [x] Operation latency tracking

## Phase 15: OpenTelemetry Tracing [COMPLETED]
- [x] Tracing integration
- [x] Span creation for all core operations
- [x] Trace context propagation
- [x] OTLP export support

## Phase 16: Time-Travel Replay Core [COMPLETED]
- [x] Replay session creation
- [x] Offset/timestamp lookup
- [x] Sandbox consumer groups
- [x] Replay state management

## Phase 17: Crash Recovery & Durability [COMPLETED]
- [x] Fsync policies (Always/Interval)
- [x] Checkpoint mechanism
- [x] Recovery orchestration on startup
- [x] Queue state reconstruction (At-Least-Once)

## Phase 18: Advanced Querying & Filtering [COMPLETED]
- [x] Custom Filter Expression Language (Parser/Evaluator)
- [x] Stream Filtering (`ReadWithOptions`)
- [x] Queue Inspection (`Peek` with filter)
- [x] KV Store Scanning (`Scan` with predicates)

## Phase 19: Web UI Foundation [PENDING]
- [ ] React + Vite setup
- [ ] Basic dashboard layout
- [ ] WebSocket connection setup
- [ ] API client integration

## Phase 20: Web UI - Queue Dashboard [PENDING]
- [ ] Queue list view
- [ ] Queue depth visualization
- [ ] Worker health display
- [ ] Job status monitoring

## Phase 21: Web UI - Stream Dashboard [PENDING]
- [ ] Stream/topic list
- [ ] Offset visualization
- [ ] Consumer group status
- [ ] Lag metrics display

## Phase 22: Web UI - Replay Interface [PENDING]
- [ ] Replay session creation UI
- [ ] Offset/timestamp picker
- [ ] Replay controls (start/pause/stop)

## Phase 23: Networking & Clustering (Future)
- [ ] Node discovery
- [ ] Partition replication (Raft/Paxos)
- [ ] Leader election

## Phase 24-30: SDKs (Node.js, Python, Go) [PENDING]
- [ ] gRPC Client generation
- [ ] Idiomatic wrappers