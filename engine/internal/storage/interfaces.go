package storage

import (
	"context"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/schema"
)

// Lifecycle manages component lifecycle
type Lifecycle interface {
	// Start initializes and starts the component
	Start(ctx context.Context) error
	// Stop gracefully stops the component
	Stop(ctx context.Context) error
	// Ready returns true if the component is ready
	Ready() bool
}

// StreamWriter defines the interface for writing to streams
type StreamWriter interface {
	// WriteEvents writes events to a stream and returns assigned offsets
	WriteEvents(ctx context.Context, resourcePath string, events []StreamEvent) ([]int64, error)
}

// StreamReader defines the interface for reading from streams
type StreamReader interface {
	// ReadFromOffset reads messages from a stream starting at the given offset
	ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error)
	// GetLatestOffset returns the latest offset for a partition
	GetLatestOffset(ctx context.Context, resourcePath string, partition int32) (int64, error)
}

// StreamManager defines the interface for stream management
type StreamManager interface {
	StreamWriter
	StreamReader
	Lifecycle
	// InitializeStream initializes a stream by loading state from disk
	InitializeStream(ctx context.Context, resourcePath string) error
}

// QueueWriter defines the interface for enqueueing jobs
type QueueWriter interface {
	// Enqueue enqueues a job to a queue
	Enqueue(ctx context.Context, resourcePath string, payload []byte, options QueueEnqueueOptions) (string, int64, error)
}

// QueueReader defines the interface for reading from queues
type QueueReader interface {
	// Reserve reserves a job from the queue with visibility timeout
	Reserve(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*QueueJob, error)
	// PopReadyJob pops a ready job from the heap (if visible)
	PopReadyJob(ctx context.Context, resourcePath string) (*QueueJob, error)
}

// QueueManager defines the interface for queue management
type QueueManager interface {
	QueueWriter
	QueueReader
	Lifecycle
	// InitializeQueue initializes a queue by loading state from disk
	InitializeQueue(ctx context.Context, resourcePath string) error
	// AddToInFlight moves a job from ReadyHeap to InFlight
	AddToInFlight(ctx context.Context, resourcePath string, jobID string, visibilityTimeout time.Duration) (*QueueJob, error)
	// RemoveFromInFlight removes a job from InFlight (used for ACK)
	RemoveFromInFlight(ctx context.Context, resourcePath string, jobID string) error
	// GetInFlight retrieves a job from InFlight
	GetInFlight(ctx context.Context, resourcePath string, jobID string) (*QueueJob, error)
	// NACK negatively acknowledges a job (requeue with backoff)
	NACK(ctx context.Context, resourcePath string, jobID string) error
	// NACKWithDelay negatively acknowledges a job with explicit delay
	NACKWithDelay(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error
	// GetJobPayload retrieves the payload for a job from the log
	GetJobPayload(ctx context.Context, resourcePath string, jobID string) ([]byte, error)
	// GetQueueStats returns statistics for a queue
	GetQueueStats(ctx context.Context, resourcePath string) (*QueueStats, error)
	// Receive receives multiple jobs from the queue (batch receive)
	Receive(ctx context.Context, resourcePath string, maxJobs int, options QueueReserveOptions) ([]*QueueJob, error)
	// SetRetryPolicy sets the retry policy for a queue
	SetRetryPolicy(ctx context.Context, resourcePath string, policy queues.RetryPolicy) error
	// GetRetryPolicy gets the retry policy for a queue
	GetRetryPolicy(ctx context.Context, resourcePath string) (*queues.RetryPolicy, error)
	// MoveToDLQ moves a job to the dead-letter queue
	MoveToDLQ(ctx context.Context, resourcePath string, jobID string) error
	// GetDLQPath gets or creates the DLQ path for a queue
	GetDLQPath(ctx context.Context, resourcePath string) (string, error)
	// ListDLQJobs lists jobs in the DLQ
	ListDLQJobs(ctx context.Context, resourcePath string, maxJobs int) ([]*QueueJob, error)
}

// QueueStats represents statistics for a queue
type QueueStats struct {
	TotalJobs     int64
	PendingJobs   int64
	InFlightJobs  int64
	CompletedJobs int64
	FailedJobs    int64
	OldestJobAge  time.Duration
}

// QueueReserveOptions specifies options for reserving/receiving jobs
type QueueReserveOptions struct {
	VisibilityTimeout time.Duration
	LongPollTimeout   time.Duration
	MaxWaitTime       time.Duration
}

// KVSetOptions specifies options for Set operations
type KVSetOptions struct {
	// TTL is the time-to-live duration (0 = no expiration)
	TTL time.Duration
}

// KVManager defines the interface for KV store management
type KVManager interface {
	Lifecycle
	// Set sets a key-value pair in the KV store
	Set(ctx context.Context, resourcePath, key string, value []byte, options KVSetOptions) error
	// Get retrieves a value by key from the KV store
	Get(ctx context.Context, resourcePath, key string) ([]byte, error)
	// Delete deletes a key from the KV store
	Delete(ctx context.Context, resourcePath, key string) error
	// Exists checks if a key exists in the KV store
	Exists(ctx context.Context, resourcePath, key string) (bool, error)
	// ListKeys lists all keys in a KV store, optionally filtered by prefix
	ListKeys(ctx context.Context, resourcePath, prefix string) ([]string, error)
	// InitializeKVStore initializes a KV store by opening its database
	InitializeKVStore(ctx context.Context, resourcePath string) error
}

// ConsumerGroupState represents the state of a consumer group (exposed in interface)
type ConsumerGroupState struct {
	Stream          string
	Group           string
	Partition       int32
	CommittedOffset int64
	LatestOffset    int64
	Lag             int64
}

// ConsumerGroupManager defines the interface for consumer group management
type ConsumerGroupManager interface {
	Lifecycle
	// CommitOffset commits an offset for a consumer group
	CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error
	// GetCommittedOffset retrieves the committed offset for a consumer group
	GetCommittedOffset(ctx context.Context, stream, group string, partition int32) (int64, error)
	// GetConsumerGroupState retrieves the complete state of a consumer group
	GetConsumerGroupState(ctx context.Context, stream, group string, partition int32) (*ConsumerGroupState, error)
	// CalculateLag calculates the lag for a consumer group
	CalculateLag(ctx context.Context, stream, group string, partition int32) (int64, error)
	// ListConsumerGroups returns all consumer groups for a stream
	ListConsumerGroups(ctx context.Context, stream string) ([]string, error)
	// DeleteConsumerGroup deletes all offsets for a consumer group
	DeleteConsumerGroup(ctx context.Context, stream, group string) error
}

// SchemaRegistry defines the interface for schema management
type SchemaRegistry interface {
	Lifecycle
	// RegisterSchema registers a new schema version
	RegisterSchema(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error
	// GetSchema retrieves a specific schema version
	GetSchema(ctx context.Context, tenant, schemaID string, version int32) (*schema.Schema, error)
	// GetLatestSchema retrieves the latest version of a schema
	GetLatestSchema(ctx context.Context, tenant, schemaID string) (*schema.Schema, error)
	// ListSchemas lists all schemas, optionally filtered by schema ID
	ListSchemas(ctx context.Context, tenant, schemaID string) ([]*schema.Schema, error)
	// DeleteSchema deletes a specific schema version
	DeleteSchema(ctx context.Context, tenant, schemaID string, version int32) error
}

// StorageBackend defines the interface for storage operations
type StorageBackend interface {
	Lifecycle
	// MetaStore returns the metadata store
	MetaStore() *metastore.Store
	// LogManager returns the log manager
	LogManager() LogManager
	// StreamManager returns the stream manager
	StreamManager() StreamManager
	// QueueManager returns the queue manager
	QueueManager() QueueManager
	// KVManager returns the KV store manager
	KVManager() KVManager
	// ConsumerGroupManager returns the consumer group manager
	ConsumerGroupManager() ConsumerGroupManager
	// SchemaRegistry returns the schema registry
	SchemaRegistry() SchemaRegistry
	// Paths returns the storage paths
	Paths() *StoragePaths
	// Validate validates the storage system integrity
	Validate(ctx context.Context) error
}

// LogManager defines the interface for log management
type LogManager interface {
	// OpenSegment opens or creates a segment for a resource and partition
	OpenSegment(resourcePath string, partition int32) (*log.SegmentWriter, error)
	// ListSegments lists all segments for a resource and partition
	ListSegments(resourcePath string, partition int32) ([]*log.SegmentMetadata, error)
	// FlushAll flushes all open segments
	FlushAll() error
	// CloseAll closes all open segments
	CloseAll() error
}

// StreamEvent represents an event to be written to a stream
type StreamEvent struct {
	Payload []byte
	Headers map[string]string
}

// QueueEnqueueOptions specifies options for enqueueing a job
type QueueEnqueueOptions struct {
	Delay   time.Duration
	Headers map[string]string
}

// QueueJob represents a job in the queue
type QueueJob struct {
	ID           string
	Seq          int64
	VisibleAt    time.Time
	ReserveUntil time.Time
	PayloadPos   log.FilePointer
	Attempts     int32
	CreatedAt    time.Time
}
