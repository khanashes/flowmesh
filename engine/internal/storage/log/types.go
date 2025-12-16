package log

import (
	"time"
)

// MessageType represents the type of message
type MessageType string

const (
	// MessageTypeStream represents a stream message
	MessageTypeStream MessageType = "STREAM"
	// MessageTypeQueue represents a queue message
	MessageTypeQueue MessageType = "QUEUE"
	// MessageTypeKVEvent represents a KV event message
	MessageTypeKVEvent MessageType = "KV_EVENT"
	// MessageTypeQueueAck represents a queue acknowledgement message
	MessageTypeQueueAck MessageType = "QUEUE_ACK"
)

// Message represents a message stored in the log
type Message struct {
	// ID is a globally unique message identifier
	ID string
	// ResourcePath is the path of the resource (tenant/namespace/type/name)
	ResourcePath string
	// Partition is the partition ID (for future sharding)
	Partition int32
	// Offset is the monotonically increasing offset per partition (for streams)
	Offset int64
	// Seq is the monotonic sequence number per queue-log (for queues)
	Seq int64
	// Type is the message type
	Type MessageType
	// Payload is the raw message payload
	Payload []byte
	// Headers contains metadata, tracing, content-type, etc.
	Headers map[string]string
	// CreatedAt is when the message was created
	CreatedAt time.Time
	// VisibleAt is when the message becomes visible (for queues with delay/visibility timeout)
	VisibleAt time.Time
	// Attempts is the number of delivery attempts (for queues)
	Attempts int32
	// SchemaVersion is the schema version if schema validation is enabled
	SchemaVersion int32
}

// FilePointer points to a location in a file
type FilePointer struct {
	// File is the file path
	File string
	// Offset is the byte offset within the file
	Offset int64
}

// SegmentMetadata contains metadata about a log segment
type SegmentMetadata struct {
	// Path is the full path to the segment file
	Path string
	// StartOffset is the first offset in this segment
	StartOffset int64
	// EndOffset is the last offset in this segment (or -1 if unknown)
	EndOffset int64
	// StartSeq is the first sequence number in this segment (for queues)
	StartSeq int64
	// EndSeq is the last sequence number in this segment (or -1 if unknown)
	EndSeq int64
	// Size is the current size of the segment file in bytes
	Size int64
	// CreatedAt is when the segment was created
	CreatedAt time.Time
	// MessageCount is the number of messages in this segment
	MessageCount int64
}
