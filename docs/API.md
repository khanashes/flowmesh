# FlowMesh API Documentation

This document provides comprehensive API reference for FlowMesh, covering both gRPC and HTTP REST endpoints.

## Table of Contents

- [Base URLs](#base-urls)
- [Authentication](#authentication)
- [Health & Status](#health--status)
- [Streams API](#streams-api)
- [Queues API](#queues-api)
- [KV Store API](#kv-store-api)
- [Schema API](#schema-api)
- [Replay API](#replay-api)
- [Error Handling](#error-handling)

## Base URLs

- **gRPC**: `localhost:50051`
- **HTTP/REST**: `http://localhost:8080`
- **Metrics**: `http://localhost:9090/metrics`

## Authentication

FlowMesh supports token-based authentication. Include the token in requests:

### HTTP/REST
```http
Authorization: Bearer <token>
```

### gRPC
```go
metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer <token>")
```

**Note**: Currently, authentication is optional and can be configured. Health check endpoints do not require authentication.

## Health & Status

### Health Check

Check if the server is running.

#### HTTP
```http
GET /health
```

**Response:**
```json
{
  "status": {
    "code": 0,
    "message": "ok"
  }
}
```

#### gRPC
```protobuf
service HealthService {
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

### Readiness Check

Check if the server is ready to serve requests.

#### HTTP
```http
GET /ready
```

**Response:**
```json
{
  "status": {
    "code": 0,
    "message": "ready"
  },
  "ready": true
}
```

#### gRPC
```protobuf
service HealthService {
  rpc ReadinessCheck(ReadinessCheckRequest) returns (ReadinessCheckResponse);
}
```

## Streams API

### List Streams

List all streams in the system.

#### HTTP
```http
GET /api/v1/streams
```

**Response:**
```json
{
  "status": "success",
  "streams": [
    {
      "tenant": "default",
      "namespace": "test",
      "name": "events",
      "resource_type": "stream",
      "created_at": 1704067200000000000
    }
  ]
}
```

#### gRPC
Not available - use HTTP endpoint.

### Write Events

Write events to a stream.

#### HTTP
```http
POST /api/v1/streams/{tenant}/{namespace}/{name}/events
Content-Type: application/json

{
  "events": [
    {
      "payload": "SGVsbG8gV29ybGQ=",
      "headers": {
        "event_type": "user.created",
        "source": "api"
      }
    }
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "offsets": [0, 1, 2]
}
```

#### gRPC
```protobuf
service StreamService {
  rpc WriteEvents(WriteEventsRequest) returns (WriteEventsResponse);
}

message WriteEventsRequest {
  ResourcePath resource_path = 1;
  repeated Event events = 2;
}

message WriteEventsResponse {
  Status status = 1;
  repeated int64 offsets = 2;
}
```

**Example (Go):**
```go
req := &flowmeshpb.WriteEventsRequest{
  ResourcePath: &flowmeshpb.ResourcePath{
    Tenant:      "default",
    Namespace:   "test",
    ResourceType: "stream",
    Name:        "events",
  },
  Events: []*flowmeshpb.Event{
    {
      Payload: []byte("Hello World"),
      Headers: map[string]string{"event_type": "user.created"},
    },
  },
}
resp, err := streamClient.WriteEvents(ctx, req)
```

### Read Messages

Read messages from a stream starting at a specific offset.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/messages?partition=0&offset=0&max_messages=10
```

**Query Parameters:**
- `partition` (int, default: 0): Partition ID
- `offset` (int64, required): Starting offset
- `max_messages` (int, default: 10): Maximum number of messages to return

**Response:**
```json
{
  "status": "success",
  "messages": [
    {
      "id": "msg-123",
      "resource_path": "default/test/stream/events",
      "partition": 0,
      "offset": 0,
      "payload": "SGVsbG8gV29ybGQ=",
      "headers": {"event_type": "user.created"},
      "created_at": 1704067200000000000,
      "schema_version": 1
    }
  ]
}
```

#### gRPC
```protobuf
service StreamService {
  rpc ReadStream(ReadStreamRequest) returns (ReadStreamResponse);
}

message ReadStreamRequest {
  ResourcePath resource_path = 1;
  int32 partition = 2;
  int64 offset = 3;
  int32 max_messages = 4;
}
```

### Subscribe to Stream

Subscribe to a stream with consumer group support (server-side streaming).

#### HTTP
Not available - use gRPC streaming.

#### gRPC
```protobuf
service StreamService {
  rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse);
}

message SubscribeRequest {
  ResourcePath resource_path = 1;
  string consumer_group = 2;
  int32 partition = 3;
  int64 start_offset = 4;  // -1 for committed offset, -2 for latest
}
```

**Example (Go):**
```go
req := &flowmeshpb.SubscribeRequest{
  ResourcePath: &flowmeshpb.ResourcePath{
    Tenant:      "default",
    Namespace:   "test",
    ResourceType: "stream",
    Name:        "events",
  },
  ConsumerGroup: "my-consumer",
  Partition:     0,
  StartOffset:   -1,  // Start from committed offset
}

stream, err := streamClient.Subscribe(ctx, req)
for {
  resp, err := stream.Recv()
  if err == io.EOF {
    break
  }
  // Process message
  processMessage(resp.Message)
}
```

### Commit Offset

Commit an offset for a consumer group.

#### HTTP
```http
POST /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets?partition=0
Content-Type: application/json

{
  "offset": 100
}
```

**Response:**
```json
{
  "status": "success",
  "message": "offset committed successfully"
}
```

#### gRPC
```protobuf
service StreamService {
  rpc CommitOffset(CommitOffsetRequest) returns (CommitOffsetResponse);
}

message CommitOffsetRequest {
  ResourcePath resource_path = 1;
  string consumer_group = 2;
  int32 partition = 3;
  int64 offset = 4;
}
```

### Get Offset

Get the committed offset for a consumer group.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets?partition=0
```

**Response:**
```json
{
  "status": "success",
  "offset": 100
}
```

#### gRPC
```protobuf
service StreamService {
  rpc GetOffset(GetOffsetRequest) returns (GetOffsetResponse);
}

message GetOffsetResponse {
  Status status = 1;
  int64 offset = 2;  // -1 if no commits yet
}
```

### Get Latest Offset

Get the latest offset for a stream partition.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/offsets/latest?partition=0
```

**Response:**
```json
{
  "status": "success",
  "offset": 150
}
```

#### gRPC
```protobuf
service StreamService {
  rpc GetLatestOffset(GetLatestOffsetRequest) returns (GetLatestOffsetResponse);
}
```

### Get Stream Statistics

Get statistics for a stream.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/stats
```

**Response:**
```json
{
  "status": "success",
  "stats": {
    "latest_offset": 150,
    "partition": 0
  }
}
```

#### gRPC
Not available - use HTTP endpoint.

### List Consumer Groups

List all consumer groups for a stream.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups
```

**Response:**
```json
{
  "status": "success",
  "consumer_groups": [
    {
      "group": "my-consumer",
      "partition": 0,
      "committed_offset": 100,
      "latest_offset": 150,
      "lag": 50
    }
  ]
}
```

#### gRPC
Not available - use HTTP endpoint.

### Get Consumer Group State

Get the complete state of a consumer group including lag.

#### HTTP
```http
GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/state?partition=0
```

**Response:**
```json
{
  "status": "success",
  "state": {
    "stream": "default/test/stream/events",
    "group": "my-consumer",
    "partition": 0,
    "committed_offset": 100,
    "latest_offset": 150,
    "lag": 50
  }
}
```

#### gRPC
```protobuf
service StreamService {
  rpc GetConsumerGroupState(GetConsumerGroupStateRequest) returns (GetConsumerGroupStateResponse);
}
```

## Queues API

### List Queues

List all queues in the system.

#### HTTP
```http
GET /api/v1/queues
```

**Response:**
```json
{
  "status": "success",
  "queues": [
    {
      "tenant": "default",
      "namespace": "test",
      "name": "orders",
      "resource_type": "queue",
      "created_at": 1704067200000000000
    }
  ]
}
```

### Enqueue Job

Enqueue a job to a queue.

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs
Content-Type: application/json

{
  "payload": "SGVsbG8gV29ybGQ=",
  "delay_seconds": 5,
  "headers": {
    "priority": "high"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "job_id": "job-abc123",
  "seq": 42
}
```

#### gRPC
```protobuf
service QueueService {
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
}

message EnqueueRequest {
  ResourcePath resource_path = 1;
  bytes payload = 2;
  int64 delay_seconds = 3;
  map<string, string> headers = 4;
}
```

### Reserve Job

Reserve a single job from the queue.

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/reserve
Content-Type: application/json

{
  "visibility_timeout_seconds": 30,
  "long_poll_timeout_seconds": 5
}
```

**Response:**
```json
{
  "status": "success",
  "job": {
    "id": "job-abc123",
    "resource_path": "default/test/queue/orders",
    "seq": 42,
    "payload": "SGVsbG8gV29ybGQ=",
    "headers": {"priority": "high"},
    "created_at": 1704067200000000000,
    "visible_at": 1704067205000000000,
    "reserve_until": 1704067230000000000,
    "attempts": 1
  }
}
```

If no job is available:
```json
{
  "status": "success",
  "job": null
}
```

#### gRPC
```protobuf
service QueueService {
  rpc Reserve(ReserveRequest) returns (ReserveResponse);
}
```

### Receive Jobs (Batch)

Receive multiple jobs from the queue.

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/receive
Content-Type: application/json

{
  "max_jobs": 10,
  "visibility_timeout_seconds": 30,
  "long_poll_timeout_seconds": 5
}
```

**Response:**
```json
{
  "status": "success",
  "jobs": [
    {
      "id": "job-abc123",
      "resource_path": "default/test/queue/orders",
      "seq": 42,
      "payload": "SGVsbG8gV29ybGQ=",
      "headers": {"priority": "high"},
      "created_at": 1704067200000000000,
      "visible_at": 1704067205000000000,
      "reserve_until": 1704067230000000000,
      "attempts": 1
    }
  ]
}
```

#### gRPC
```protobuf
service QueueService {
  rpc Receive(ReceiveRequest) returns (ReceiveResponse);
}
```

### Acknowledge Job

Acknowledge completion of a job.

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/ack
```

**Response:**
```json
{
  "status": "success",
  "message": "job acknowledged successfully"
}
```

#### gRPC
```protobuf
service QueueService {
  rpc ACK(ACKRequest) returns (ACKResponse);
}

message ACKRequest {
  ResourcePath resource_path = 1;
  string job_id = 2;
}
```

### Negative Acknowledge Job

NACK a job (requeue with backoff).

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/nack
Content-Type: application/json

{
  "delay_seconds": 60
}
```

**Response:**
```json
{
  "status": "success",
  "message": "job NACKed successfully"
}
```

**Note**: If `delay_seconds` is omitted or 0, the retry policy backoff will be used.

#### gRPC
```protobuf
service QueueService {
  rpc NACK(NACKRequest) returns (NACKResponse);
}

message NACKRequest {
  ResourcePath resource_path = 1;
  string job_id = 2;
  int64 delay_seconds = 3;  // 0 = use backoff
}
```

### Get Queue Statistics

Get queue statistics (depth, in-flight, age, etc.).

#### HTTP
```http
GET /api/v1/queues/{tenant}/{namespace}/{name}/stats
```

**Response:**
```json
{
  "status": "success",
  "stats": {
    "total_jobs": 1000,
    "pending_jobs": 10,
    "in_flight_jobs": 5,
    "completed_jobs": 980,
    "failed_jobs": 5,
    "oldest_job_age_seconds": 3600
  }
}
```

#### gRPC
```protobuf
service QueueService {
  rpc GetQueueStats(GetQueueStatsRequest) returns (GetQueueStatsResponse);
}
```

### Set Retry Policy

Set the retry policy for a queue.

#### HTTP
```http
PUT /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
Content-Type: application/json

{
  "max_attempts": 3,
  "initial_backoff_seconds": 1,
  "max_backoff_seconds": 60,
  "backoff_multiplier": 2.0,
  "backoff_strategy": "exponential"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "retry policy set successfully"
}
```

**Backoff Strategies:**
- `fixed`: Constant delay
- `linear`: Linear increase
- `exponential`: Exponential backoff (default)

#### gRPC
```protobuf
service QueueService {
  rpc SetRetryPolicy(SetRetryPolicyRequest) returns (SetRetryPolicyResponse);
}
```

### Get Retry Policy

Get the retry policy for a queue.

#### HTTP
```http
GET /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
```

**Response:**
```json
{
  "status": "success",
  "policy": {
    "max_attempts": 3,
    "initial_backoff_seconds": 1,
    "max_backoff_seconds": 60,
    "backoff_multiplier": 2.0,
    "backoff_strategy": "exponential"
  }
}
```

#### gRPC
```protobuf
service QueueService {
  rpc GetRetryPolicy(GetRetryPolicyRequest) returns (GetRetryPolicyResponse);
}
```

### List DLQ Jobs

List jobs in the dead-letter queue.

#### HTTP
```http
GET /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs?max_jobs=100
```

**Response:**
```json
{
  "status": "success",
  "jobs": [
    {
      "id": "job-dlq-123",
      "resource_path": "default/test/queue/orders",
      "seq": 42,
      "payload": "SGVsbG8gV29ybGQ=",
      "attempts": 5
    }
  ]
}
```

#### gRPC
```protobuf
service QueueService {
  rpc ListDLQJobs(ListDLQJobsRequest) returns (ListDLQJobsResponse);
}
```

### Replay DLQ Job

Replay a job from DLQ back to the main queue.

#### HTTP
```http
POST /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs/{job_id}/replay
```

**Response:**
```json
{
  "status": "success",
  "job_id": "job-new-456",
  "seq": 1001
}
```

#### gRPC
```protobuf
service QueueService {
  rpc ReplayDLQJob(ReplayDLQJobRequest) returns (ReplayDLQJobResponse);
}
```

## KV Store API

### Set Key-Value

Set a key-value pair with optional TTL.

#### HTTP
```http
PUT /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
Content-Type: application/json

{
  "value": "SGVsbG8gV29ybGQ=",
  "ttl_seconds": 3600
}
```

**Response:**
```json
{
  "status": "success",
  "message": "key-value pair set successfully"
}
```

#### gRPC
```protobuf
service KVService {
  rpc Set(SetRequest) returns (SetResponse);
}

message SetRequest {
  ResourcePath resource_path = 1;
  string key = 2;
  bytes value = 3;
  int64 ttl_seconds = 4;  // 0 = no expiration
}
```

### Get Value

Retrieve a value by key.

#### HTTP
```http
GET /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
```

**Response:**
```json
{
  "status": "success",
  "message": "value retrieved successfully",
  "value": "SGVsbG8gV29ybGQ="
}
```

#### gRPC
```protobuf
service KVService {
  rpc Get(GetRequest) returns (GetResponse);
}
```

### Delete Key

Delete a key.

#### HTTP
```http
DELETE /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
```

**Response:**
```json
{
  "status": "success",
  "message": "key deleted successfully"
}
```

#### gRPC
```protobuf
service KVService {
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}
```

### Check if Key Exists

Check if a key exists.

#### HTTP
```http
HEAD /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
```

**Response:**
- `200 OK` if key exists
- `404 Not Found` if key does not exist

Or with JSON body:
```json
{
  "status": "success",
  "exists": true
}
```

#### gRPC
```protobuf
service KVService {
  rpc Exists(ExistsRequest) returns (ExistsResponse);
}
```

### List Keys

List all keys, optionally filtered by prefix.

#### HTTP
```http
GET /api/v1/kv/{tenant}/{namespace}/{name}/keys?prefix=user:
```

**Query Parameters:**
- `prefix` (string, optional): Filter keys by prefix

**Response:**
```json
{
  "status": "success",
  "message": "keys listed successfully",
  "keys": ["user:123", "user:456", "user:789"]
}
```

#### gRPC
```protobuf
service KVService {
  rpc ListKeys(ListKeysRequest) returns (ListKeysResponse);
}

message ListKeysRequest {
  ResourcePath resource_path = 1;
  string prefix = 2;
}
```

## Schema API

### Register Schema

Register a new schema version.

#### HTTP
```http
POST /api/v1/schemas
Content-Type: application/json

{
  "tenant": "default",
  "schema_id": "user-event",
  "schema_type": "jsonschema",
  "version": 1,
  "definition": "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "schema registered successfully"
}
```

#### gRPC
```protobuf
service SchemaService {
  rpc RegisterSchema(RegisterSchemaRequest) returns (RegisterSchemaResponse);
}
```

### Get Schema

Retrieve a schema by ID and version.

#### HTTP
```http
GET /api/v1/schemas/{schema_id}/v{version}?tenant=default
```

**Response:**
```json
{
  "status": "success",
  "schema": {
    "id": "user-event",
    "type": "jsonschema",
    "version": 1,
    "definition": "{\"type\":\"object\"}",
    "created_at": 1704067200000000000
  }
}
```

#### gRPC
```protobuf
service SchemaService {
  rpc GetSchema(GetSchemaRequest) returns (GetSchemaResponse);
}

message GetSchemaRequest {
  string tenant = 1;
  string schema_id = 2;
  int32 version = 3;  // 0 = latest
}
```

### List Schemas

List all schemas, optionally filtered by schema ID.

#### HTTP
```http
GET /api/v1/schemas?tenant=default&schema_id=user-event
```

**Query Parameters:**
- `tenant` (string, required): Tenant identifier
- `schema_id` (string, optional): Filter by schema ID

**Response:**
```json
{
  "status": "success",
  "schemas": [
    {
      "id": "user-event",
      "type": "jsonschema",
      "version": 1,
      "definition": "{\"type\":\"object\"}",
      "created_at": 1704067200000000000
    }
  ]
}
```

#### gRPC
```protobuf
service SchemaService {
  rpc ListSchemas(ListSchemasRequest) returns (ListSchemasResponse);
}
```

### Delete Schema

Delete a schema version.

#### HTTP
```http
DELETE /api/v1/schemas/{schema_id}/v{version}?tenant=default
```

**Response:**
```json
{
  "status": "success",
  "message": "schema deleted successfully"
}
```

#### gRPC
```protobuf
service SchemaService {
  rpc DeleteSchema(DeleteSchemaRequest) returns (DeleteSchemaResponse);
}
```

### Set Resource Schema

Attach a schema to a resource (stream/queue).

#### HTTP
```http
PUT /api/v1/streams/{tenant}/{namespace}/{name}/schema
Content-Type: application/json

{
  "schema_id": "user-event",
  "version": 1
}
```

**Response:**
```json
{
  "status": "success",
  "message": "schema attached successfully"
}
```

#### gRPC
```protobuf
service SchemaService {
  rpc SetResourceSchema(SetResourceSchemaRequest) returns (SetResourceSchemaResponse);
}
```

## Replay API

### Create Replay Session

Create a new replay session.

#### HTTP
```http
POST /api/v1/replay/sessions?stream={tenant}/{namespace}/{name}
Content-Type: application/json

{
  "start_offset": 0,
  "end_offset": 100,
  "sandbox_consumer_group": "_replay_debug_session_123"
}
```

Or with timestamp:
```json
{
  "start_timestamp": 1704067200000000000,
  "end_timestamp": 1704153600000000000,
  "sandbox_consumer_group": "_replay_debug_session_123"
}
```

**Response:**
```json
{
  "status": "success",
  "session": {
    "session_id": "session-abc123",
    "stream": "default/test/stream/events",
    "partition": 0,
    "start_offset": 0,
    "end_offset": 100,
    "sandbox_consumer_group": "_replay_debug_session_123",
    "status": "created",
    "progress": {
      "current_offset": 0,
      "messages_replayed": 0,
      "errors": 0
    },
    "created_at": 1704067200000000000,
    "updated_at": 1704067200000000000
  }
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc CreateReplaySession(CreateReplaySessionRequest) returns (CreateReplaySessionResponse);
}
```

### List Replay Sessions

List all replay sessions, optionally filtered by stream.

#### HTTP
```http
GET /api/v1/replay/sessions?stream={tenant}/{namespace}/{name}
```

**Query Parameters:**
- `stream` (string, optional): Filter by stream path

**Response:**
```json
{
  "status": "success",
  "sessions": [
    {
      "session_id": "session-abc123",
      "stream": "default/test/stream/events",
      "status": "active",
      "progress": {
        "current_offset": 50,
        "messages_replayed": 50,
        "errors": 0
      }
    }
  ]
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc ListReplaySessions(ListReplaySessionsRequest) returns (ListReplaySessionsResponse);
}
```

### Get Replay Session

Get details of a specific replay session.

#### HTTP
```http
GET /api/v1/replay/sessions/{session_id}
```

**Response:**
```json
{
  "status": "success",
  "session": {
    "session_id": "session-abc123",
    "stream": "default/test/stream/events",
    "partition": 0,
    "start_offset": 0,
    "end_offset": 100,
    "sandbox_consumer_group": "_replay_debug_session_123",
    "status": "active",
    "progress": {
      "current_offset": 50,
      "messages_replayed": 50,
      "errors": 0,
      "started_at": 1704067200000000000
    },
    "created_at": 1704067200000000000,
    "updated_at": 1704067250000000000
  }
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc GetReplaySession(GetReplaySessionRequest) returns (GetReplaySessionResponse);
}
```

### Start Replay

Start replaying messages for a session.

#### HTTP
```http
POST /api/v1/replay/sessions/{session_id}/start
```

**Response:**
```json
{
  "status": "success",
  "message": "replay started successfully"
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc StartReplay(StartReplayRequest) returns (StartReplayResponse);
}
```

### Pause Replay

Pause an active replay session.

#### HTTP
```http
POST /api/v1/replay/sessions/{session_id}/pause
```

**Response:**
```json
{
  "status": "success",
  "message": "replay paused successfully"
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc PauseReplay(PauseReplayRequest) returns (PauseReplayResponse);
}
```

### Resume Replay

Resume a paused replay session.

#### HTTP
```http
POST /api/v1/replay/sessions/{session_id}/resume
```

**Response:**
```json
{
  "status": "success",
  "message": "replay resumed successfully"
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc ResumeReplay(ResumeReplayRequest) returns (ResumeReplayResponse);
}
```

### Stop Replay

Stop an active replay session.

#### HTTP
```http
POST /api/v1/replay/sessions/{session_id}/stop
```

**Response:**
```json
{
  "status": "success",
  "message": "replay stopped successfully"
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc StopReplay(StopReplayRequest) returns (StopReplayResponse);
}
```

### Delete Replay Session

Delete a replay session.

#### HTTP
```http
DELETE /api/v1/replay/sessions/{session_id}
```

**Response:**
```json
{
  "status": "success",
  "message": "replay session deleted successfully"
}
```

#### gRPC
```protobuf
service ReplayService {
  rpc DeleteReplaySession(DeleteReplaySessionRequest) returns (DeleteReplaySessionResponse);
}
```

## Error Handling

### Error Response Format

All errors follow a consistent format:

#### HTTP
```json
{
  "status": "error",
  "message": "stream not found: default/test/stream/events",
  "code": 404
}
```

#### gRPC
```protobuf
message Status {
  int32 code = 1;      // gRPC status code
  string message = 2;  // Human-readable error message
  string details = 3;  // Additional error details
}
```

### Common Error Codes

| HTTP Status | gRPC Code | Description |
|-------------|-----------|-------------|
| 200 | OK | Success |
| 400 | InvalidArgument | Invalid request parameters |
| 401 | Unauthenticated | Authentication required |
| 403 | PermissionDenied | Insufficient permissions |
| 404 | NotFound | Resource not found |
| 409 | AlreadyExists | Resource already exists |
| 429 | ResourceExhausted | Rate limit exceeded |
| 500 | Internal | Internal server error |
| 503 | Unavailable | Service unavailable |

### Common Error Messages

- `stream not found: {path}` - Stream does not exist
- `queue not found: {path}` - Queue does not exist
- `kv store not found: {path}` - KV store does not exist
- `consumer group not found: {group}` - Consumer group does not exist
- `invalid offset: {offset}` - Invalid offset value
- `key not found: {key}` - Key does not exist in KV store
- `key expired: {key}` - Key has expired (TTL)
- `job not found: {job_id}` - Job does not exist
- `replay session not found: {session_id}` - Replay session does not exist

## Rate Limiting

Rate limiting is currently not implemented but may be added in future versions.

## Pagination

List endpoints may support pagination in future versions. Currently, they return all results.

## Resource Path Format

Resources are identified using hierarchical paths:

```
{tenant}/{namespace}/{resource_type}/{name}
```

Examples:
- Stream: `default/test/stream/events`
- Queue: `default/production/queue/orders`
- KV Store: `tenant1/staging/kv/cache`

**Note**: The `resource_type` is included in the path for clarity but may be inferred from the API endpoint used.

## Timestamps

All timestamps are Unix nanoseconds (int64):
- Example: `1704067200000000000` = December 30, 2023, 00:00:00 UTC

## Payload Encoding

HTTP payloads are base64-encoded strings. Decode before use:

```javascript
const payload = atob(response.value);  // JavaScript
```

```python
import base64
payload = base64.b64decode(response['value'])  # Python
```

## SDK Examples

For higher-level SDK usage, see:
- [Go SDK README](../sdk-go/README.md)
- [Python SDK README](../sdk-python/README.md)

