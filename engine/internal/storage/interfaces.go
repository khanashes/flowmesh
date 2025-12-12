package storage

import (
	"context"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
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
