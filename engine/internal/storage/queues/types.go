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

// ReserveOptions specifies options for reserving a job
type ReserveOptions struct {
	// VisibilityTimeout is how long the job will be invisible after reservation
	VisibilityTimeout time.Duration
	// LongPollTimeout is how long to wait for a job to become available (0 = no wait)
	LongPollTimeout time.Duration
	// MaxWaitTime is the maximum time to wait for a job (for long polling)
	MaxWaitTime time.Duration
}

// DefaultReserveOptions returns default reserve options
func DefaultReserveOptions() ReserveOptions {
	return ReserveOptions{
		VisibilityTimeout: 30 * time.Second,
		LongPollTimeout:   0, // No long polling by default
		MaxWaitTime:       20 * time.Second,
	}
}

// RetryPolicy defines retry behavior for failed jobs
type RetryPolicy struct {
	// MaxAttempts is the maximum number of delivery attempts (0 = unlimited)
	MaxAttempts int32
	// InitialBackoff is the initial delay before retry
	InitialBackoff time.Duration
	// MaxBackoff is the maximum delay between retries
	MaxBackoff time.Duration
	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64
	// BackoffStrategy is the strategy for calculating backoff
	BackoffStrategy BackoffStrategy
}

// BackoffStrategy defines how backoff is calculated
type BackoffStrategy string

const (
	// BackoffStrategyFixed uses a fixed delay
	BackoffStrategyFixed BackoffStrategy = "fixed"
	// BackoffStrategyLinear increases delay linearly
	BackoffStrategyLinear BackoffStrategy = "linear"
	// BackoffStrategyExponential increases delay exponentially
	BackoffStrategyExponential BackoffStrategy = "exponential"
)

// DefaultRetryPolicy returns a default retry policy
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        60 * time.Second,
		BackoffMultiplier: 2.0,
		BackoffStrategy:   BackoffStrategyExponential,
	}
}

// WorkerInfo represents information about a worker
type WorkerInfo struct {
	// WorkerID is a unique identifier for the worker
	WorkerID string
	// QueuePath is the resource path of the queue
	QueuePath string
	// LastHeartbeat is when the worker last sent a heartbeat
	LastHeartbeat time.Time
	// ActiveJobs is a list of job IDs currently being processed
	ActiveJobs []string
}

// QueueStats represents statistics for a queue
type QueueStats struct {
	// TotalJobs is the total number of jobs ever enqueued
	TotalJobs int64
	// PendingJobs is the number of jobs in ReadyHeap
	PendingJobs int64
	// InFlightJobs is the number of jobs currently being processed
	InFlightJobs int64
	// CompletedJobs is the number of successfully completed jobs
	CompletedJobs int64
	// FailedJobs is the number of failed jobs
	FailedJobs int64
	// OldestJobAge is the age of the oldest pending job
	OldestJobAge time.Duration
}
