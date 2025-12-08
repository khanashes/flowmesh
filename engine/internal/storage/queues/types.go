package queues

import (
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
)

// JobMetadata represents metadata for a job in the queue
type JobMetadata struct {
	// ID is a unique job identifier
	ID string
	// Seq is the sequence number in the queue log
	Seq int64
	// VisibleAt is when the job becomes visible (for delayed jobs)
	VisibleAt time.Time
	// ReserveUntil is when the visibility timeout expires (for in-flight jobs)
	ReserveUntil time.Time
	// PayloadPos points to the location of the job payload in the log
	PayloadPos log.FilePointer
	// Attempts is the number of delivery attempts
	Attempts int32
	// CreatedAt is when the job was created
	CreatedAt time.Time
}

// EnqueueOptions specifies options for enqueueing a job
type EnqueueOptions struct {
	// Delay is the delay before the job becomes visible
	Delay time.Duration
	// Headers contains metadata for the job
	Headers map[string]string
}

// DefaultEnqueueOptions returns default enqueue options
func DefaultEnqueueOptions() EnqueueOptions {
	return EnqueueOptions{
		Delay:   0,
		Headers: make(map[string]string),
	}
}
