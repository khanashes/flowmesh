Nice, we’re going straight into the guts 😈
I’ll give you a real engine-level design you could actually implement in Go.

I’ll focus on Phase 1 (single-node) but design it so Phase 2 (clustered) is a natural extension.

⸻

0. Mental model

FlowMesh node = one binary exposing:
	•	Control API (gRPC/HTTP)
	•	Data plane (streams, queues, kv)
	•	Embedded storage engine (log + KV)
	•	Background schedulers (retries, TTL, compaction)
	•	Metrics & tracing exporters

Everything else is layers on top.

⸻

1. Core entities & data model

1.1. Tenant & namespace

You’ll probably want multi-tenancy:
	•	Tenant: top-level isolation (e.g. company / project)
	•	Namespace: logical grouping within tenant (e.g. prod, staging)

Everything is addressed as:

tenant / namespace / resource_type / resource_name

1.2. Resource types
	1.	KV
	•	kv:tenant:namespace:name
	2.	Queue (jobs)
	•	queue:tenant:namespace:name
	3.	Stream (events)
	•	stream:tenant:namespace:name

1.3. Message object

Everything stored in logs is some variant of:

type Message struct {
    ID            string            // globally unique
    ResourcePath  string            // tenant/ns/stream-or-queue-name
    Partition     int32             // for future sharding
    Offset        int64             // monotonically increasing per partition (streams)
    Seq           int64             // monotonic per queue-log
    Type          MessageType       // enum: STREAM, QUEUE, KV_EVENT
    Payload       []byte            // raw bytes
    Headers       map[string]string // metadata, tracing, content-type
    CreatedAt     time.Time
    VisibleAt     time.Time         // for queues (delayed / visibility timeout)
    Attempts      int32             // delivery attempts (queues)
    SchemaVersion int32             // if using schema validation
}

You’ll have different state & indexes per resource type, but a common underlying log/persistence.

⸻

2. Storage engine design

2.1. High-level storage approach

Use append-only segmented logs on disk, plus small indexes in memory & on disk:
	•	Disk layout (single node):

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

2.2. Streams storage

For each stream:
	•	Maintain partition(s) – MVP can be partition=0 for all streams.
	•	For each partition:
	•	One or more log segments (rotated by size/time).
	•	A separate offset index (offset → file, position).

Segment file format (simplified):

[EntryLength][EntryBytes][Checksum] repeated

EntryBytes = binary-encoded Message (without heavy fields like Headers if needed).

Offset index:

type OffsetIndex struct {
    // sparse index: every Nth message
    Entries []struct {
        Offset int64
        File   string
        Pos    int64
    }
}

On startup, you:
	•	Scan segments,
	•	Rebuild latest offset counters,
	•	Load sparse indexes into memory.

2.3. Queue storage

You can reuse the stream log concept, but queue semantics need extra state:
	•	Log of all enqueued jobs (append-only)
	•	Separate in-memory priority index for:
	•	Not-yet-visible messages (delayed / visibility timeout)
	•	Pending messages per queue

Two core structures:

type QueueState struct {
    Name         string
    NextSeq      int64
    ReadyHeap    *JobMinHeap       // by VisibleAt (soonest first)
    InFlight     map[string]*Job   // messageID -> JobMetadata
    DeadLetter   *DeadLetterState
}

type Job struct {
    ID         string
    Seq        int64
    VisibleAt  time.Time
    PayloadPos FilePointer // file + offset in log
    Attempts   int32
}

Process:
	•	On enqueue: append to log, push into ReadyHeap with VisibleAt.
	•	On worker pull: pop ReadyHeap where VisibleAt <= now, add to InFlight.
	•	On ACK: mark as done, remove from InFlight, maybe write “ack” marker (or treat as idempotent at consumer side).
	•	On timeout / NACK: return to ReadyHeap with updated VisibleAt and Attempts.

Dead-letter is another queue/stream under the hood.

2.4. KV storage

Use an embedded KV DB (e.g. Pebble/Badger):
	•	Key: tenant:namespace:kv:<name>:<user_key>
	•	Value: encoded bytes, metadata (TTL etc.)

TTL handling:
	•	Store expires_at inside value OR in a separate expiry index.
	•	Periodic background scan deletes expired keys.

⸻

3. Message lifecycle – Write & Read paths

3.1. Streams – write path
	1.	Client calls WriteEvents(stream, events[]).
	2.	Server:
	•	Validates stream exists & schema (if enabled).
	•	For each event:
	•	Assign Offset = nextOffset++ for that partition.
	•	Encode Message → bytes.
	•	Append to active segment using FileAppender.
	•	Update in-memory offset counters & sparse index.
	3.	Returns offsets to client.

3.2. Streams – read path

Two modes:
	1.	Pull (polling) – like read_from(offset, max_n).
	2.	Server-side streaming – gRPC stream using offsets.

For each consumer group:

type ConsumerGroupState struct {
    Stream       string
    Group        string
    Partition    int32
    CommittedOffset int64 // last committed offset
    Lag          int64    // derived
}

	•	On Subscribe(stream, group):
	•	Load group offset from metadata store.
	•	Start a StreamReader goroutine per partition, reading from CommittedOffset+1.
	•	Push messages to consumer channel.
	•	On Ack(group, offset):
	•	Update CommittedOffset and flush to disk (metadata).

This is your basis for replay:
	•	To replay, create a sandbox consumer group starting from older offset.

⸻

3.3. Queues – enqueue path
	1.	Client calls Enqueue(queueName, payload, options).
	2.	Server:
	•	Wrap as Message with Type=QUEUE.
	•	Set VisibleAt = now + delay.
	•	Append to queue log (like stream).
	•	Insert pointer into ReadyHeap with VisibleAt.
	3.	Return job ID / seq.

3.4. Queues – worker receive path

Workers call Reserve(queueName, options) / Receive() via:
	•	Long-polling HTTP/gRPC streaming.

Server flow:
	1.	Pop job from ReadyHeap where VisibleAt <= now.
	•	If none available, block or timeout.
	2.	Mark job in InFlight with:
	•	ReserveUntil = now + visibilityTimeout.
	3.	Return job payload + jobID.

Separate background goroutine:
	•	Every X seconds, scan InFlight:
	•	If time.Now() > ReserveUntil:
	•	If Attempts < MaxAttempts: requeue into ReadyHeap with new VisibleAt.
	•	Else: send to DLQ.

3.5. Queues – ACK & NACK
	•	ACK:
	•	Remove from InFlight.
	•	Optionally append “done” marker to log (for crash recovery).
	•	NACK:
	•	Like visibility timeout expiry – reinsert into ReadyHeap with backoff.

⸻

3.6. KV – read/write path

Pretty straightforward:
	•	Set:
	•	Validate key & namespace.
	•	Prepare value struct {payload, expiresAt}.
	•	Put into embedded DB.
	•	Get:
	•	Lookup; if expired, delete and return miss.
	•	Background TTL cleaner:
	•	Periodic scan by prefix + expiry index.

⸻

4. Metadata & config subsystem

You need a central metadata store (can just be a JSON file + small KV store in MVP):

4.1. Resource metadata

type ResourceType string
const (
    ResourceStream ResourceType = "stream"
    ResourceQueue  ResourceType = "queue"
    ResourceKV     ResourceType = "kv"
)

type ResourceConfig struct {
    Tenant      string
    Namespace   string
    Name        string
    Type        ResourceType
    Partitions  int
    Retention   RetentionConfig
    DLQ         *DLQConfig       // for queues
    Schema      *SchemaRef       // for streams/queues
    CreatedAt   time.Time
}

Stored in e.g. metadata/resources.json or dedicated kv prefix.

4.2. Consumer metadata

type ConsumerOffset struct {
    Stream      string
    Group       string
    Partition   int32
    Offset      int64
    UpdatedAt   time.Time
}

Persisted in metadata/consumers.json or kv.

4.3. Schemas

Option 1 (simple): store raw JSON Schema per stream/queue:

type SchemaRef struct {
    ID          string
    Type        string // "jsonschema"
    Version     int32
    Definition  []byte // raw JSON
}

Validations:
	•	On write: validate the payload.
	•	Optionally: “warn-only” mode (log invalid, but still accept).

⸻

5. Concurrency model (Go-level design)

Think in per-resource goroutines + shared services.

5.1. Core components
	•	LogManager
	•	Manages open segment files.
	•	Handles append, rotate, flush.
	•	StreamManager
	•	Manages streams, partitions, consumer groups.
	•	For each (stream, partition):
	•	Reader goroutine for each consumer group (server-side streaming).
	•	QueueManager
	•	For each queue:
	•	ReadyHeap, InFlight map.
	•	Scheduler goroutines:
	•	visibilityTimeoutWatcher
	•	delayedJobActivator (if needed separate)
	•	KVStore
	•	Thin wrapper over embedded DB, plus TTL goroutine.
	•	MetaStore
	•	In-memory cache + on-disk persistence for resource configs, consumer offsets, schema data.
	•	APIServer
	•	HTTP/gRPC endpoints.
	•	Authentication / authorization.
	•	Request → call into managers.
	•	MetricsExporter
	•	Periodic metrics: queue length, lag, throughput, errors.

5.2. Goroutine sketch (per node)

main
 ├─ APIServer
 ├─ MetricsExporter
 ├─ TTLReaper (KV)
 ├─ QueueScheduler (per queue)
 │    ├─ visibility watcher
 │    └─ delayed job promoter
 └─ StreamReaders (per stream-partition-consumerGroup)

Use channels & context cancellation to manage shutdown.

⸻

6. Backpressure & flow control

You want simple, predictable behavior.

6.1. Queue backpressure
	•	Limit max in-flight jobs per worker based on Reserve calls.
	•	Limit queue size (max messages). If exceeded:
	•	Return “queue full” error,
	•	Or block (configurable).

Expose metrics:
	•	queue_messages_total
	•	queue_ready_messages
	•	queue_inflight_messages
	•	queue_oldest_ready_age

These feed autoscalers later.

6.2. Stream backpressure

For streaming consumers:
	•	Control max outstanding messages per stream of gRPC.
	•	Use sliding window of unacked offsets.
	•	If consumer falls far behind:
	•	Optionally drop connection or switch to batch/pull mode.

⸻

7. Time-travel & replay design

This is the killer feature, so design it cleanly.

7.1. Replay model

A “replay session” is just a special consumer group with:
	•	mode = sandbox
	•	start_offset or start_time
	•	end_offset / end_time (optional)
	•	optional target_endpoint (to call downstream HTTP/GRPC worker instead of your normal worker cluster).

You do not mutate the log; you just:
	•	compute startOffset from timestamp → scan index & segments,
	•	create a ReplayConsumerGroup,
	•	stream messages through.

7.2. Safety

Replay must not accidentally re-run side effects in production.

So:
	•	Replay consumers must:
	•	run with different credentials / environment,
	•	or tag messages with x-flowmesh-replay=true,
	•	or hit a sandbox URL.

You can enforce in engine:
	•	“Replay groups cannot reuse production consumer group names.”

⸻

8. Crash recovery & durability

8.1. Durability policy

Configurable per-node:
	•	fsync_every_write (slower, safer)
	•	fsync_on_batch (default, tradeoff)
	•	fsync_interval_ms (e.g. at most every 5-10ms)

On crash:
	•	At restart, you:
	1.	Scan segments from last known checkpoint.
	2.	Rebuild:
	•	Latest offsets
	•	Queue ReadyHeap and InFlight:
	•	Anything “in-flight” without an ACK marker can be:
	•	Re-queued,
	•	Or considered lost (depending on semantics).

8.2. Queue recovery

Persist minimal metadata per job:
	•	job ID, seq, attempts, visibleAt, finished flag.

Two options:
	1.	Log-based:
	•	Log append: ENQUEUE, ACK, DEADLETTER.
	•	On recovery, replay log to rebuild states.
	2.	Side metadata store:
	•	Maintain additional small KV store keyed by job ID.

For MVP, log-based replay is fine.

⸻

9. Security & multi-tenancy

Even in MVP you want at least:
	•	API keys / tokens with scoped permissions:
	•	Tenant ID
	•	Allowed namespaces
	•	Allowed actions (read/write/admin)

Simple model:

type APIToken struct {
    TokenHash    string
    Tenant       string
    AllowedNS    []string
    Permissions  []string // "kv.read", "queue.write", "stream.read" etc.
}

Request flow:
	•	Parse auth header → look up token → attach AuthContext to request.
	•	All managers check AuthContext against resource path.

⸻

10. Clustering (Phase 2 preview)

Design your engine assuming you’ll later add several nodes.

10.1. Partitions & ownership

Each resource gets N partitions:
	•	Streams: partitioned by key/hash.
	•	Queues: partitioned similarly or single partition often enough.

A PartitionState struct:

type PartitionState struct {
    ResourcePath string
    PartitionID  int32
    LeaderNode   string
    Followers    []string
}

You’ll introduce:
	•	A Cluster Manager (could be Raft-based) to:
	•	elect leaders,
	•	assign partitions to nodes,
	•	manage membership.

10.2. Raft groups

At some point:
	•	Each partition becomes a Raft group:
	•	Logs are replicated across nodes.
	•	Only leader accepts writes.
	•	Followers replay log for consistency.

Your existing log & index code can be reused inside each Raft FSM.

10.3. Client requests in cluster & routing
	•	Clients talk to:
	•	a front-end “gateway”,
	•	or directly to nodes (after they get partition map).
	•	Gateway logic:
	•	On Write(stream, key), choose partition via hash(key) → locate leader node → forward.
	•	This is why designing Partition as a first-class field in Message is important even in MVP.

⸻

11. Observability & metrics detail

11.1. Metrics to expose

Per queue:
	•	flowmesh_queue_ready_total
	•	flowmesh_queue_inflight_total
	•	flowmesh_queue_dead_letter_total
	•	flowmesh_queue_oldest_ready_age_seconds
	•	flowmesh_queue_process_duration_seconds (histogram)
	•	flowmesh_queue_retry_total

Per stream:
	•	flowmesh_stream_messages_total
	•	flowmesh_stream_consumer_lag{group=...}
	•	flowmesh_stream_throughput_messages_per_second

Node-wide:
	•	flowmesh_node_disk_bytes_used
	•	flowmesh_node_file_descriptors_used
	•	flowmesh_node_go_goroutines

11.2. Tracing

Export tracing with OpenTelemetry:
	•	Each operation becomes a span:
	•	flowmesh.stream.write
	•	flowmesh.stream.read
	•	flowmesh.queue.enqueue
	•	flowmesh.queue.reserve
	•	flowmesh.queue.ack

Propagate trace_id / span_id from headers into Message metadata.

⸻

12. How to start implementing (practical order)

Given all of this, here’s the engine build order I’d suggest:
	1.	MetaStore + ResourceConfig
	•	Create, delete, list streams/queues/kv namespaces.
	2.	LogManager + Segment files
	•	Implement append-only logs + indexes with tests.
	3.	Streams (single partition)
	•	Write/read by offset,
	•	Consumer groups with committed offsets.
	4.	QueueManager
	•	Build ReadyHeap, InFlight, retries, DLQ.
	•	Expose gRPC/HTTP endpoints for Enqueue/Reserve/Ack.
	5.	KVStore
	•	Embed Pebble/Badger, simple prefix-based keys, TTL.
	6.	Replay / Sandbox consumer groups
	•	“Create replay session” → special consumer group.
	7.	Metrics + basic tracing
	8.	Minimal web UI
	•	Just show queue depths, lags, and allow starting replay session.

Once that’s stable, then think about clustering.
