package queues

import (
	"fmt"
	"time"
)

// QueueNotFoundError indicates a queue resource was not found
type QueueNotFoundError struct {
	ResourcePath string
}

func (e QueueNotFoundError) Error() string {
	return fmt.Sprintf("queue not found: %s", e.ResourcePath)
}

// JobNotFoundError indicates a job was not found
type JobNotFoundError struct {
	JobID        string
	ResourcePath string
}

func (e JobNotFoundError) Error() string {
	return fmt.Sprintf("job %s not found in queue %s", e.JobID, e.ResourcePath)
}

// EnqueueError indicates an error enqueueing a job
type EnqueueError struct {
	ResourcePath string
	Err          error
}

func (e EnqueueError) Error() string {
	return fmt.Sprintf("failed to enqueue job to queue %s: %v", e.ResourcePath, e.Err)
}

func (e EnqueueError) Unwrap() error {
	return e.Err
}

// InvalidVisibilityTimeoutError indicates an invalid visibility timeout
type InvalidVisibilityTimeoutError struct {
	Timeout time.Duration
	Reason  string
}

func (e InvalidVisibilityTimeoutError) Error() string {
	return fmt.Sprintf("invalid visibility timeout %v: %s", e.Timeout, e.Reason)
}

// NACKError indicates an error during NACK operation
type NACKError struct {
	ResourcePath string
	JobID        string
	Err          error
}

func (e NACKError) Error() string {
	return fmt.Sprintf("failed to NACK job %s in queue %s: %v", e.JobID, e.ResourcePath, e.Err)
}

func (e NACKError) Unwrap() error {
	return e.Err
}

// MaxAttemptsExceededError indicates a job exceeded max retry attempts
type MaxAttemptsExceededError struct {
	JobID        string
	ResourcePath string
	Attempts     int32
	MaxAttempts  int32
}

func (e MaxAttemptsExceededError) Error() string {
	return fmt.Sprintf("job %s exceeded max attempts (%d/%d) in queue %s", e.JobID, e.Attempts, e.MaxAttempts, e.ResourcePath)
}

// WorkerNotFoundError indicates a worker was not found
type WorkerNotFoundError struct {
	WorkerID     string
	ResourcePath string
}

func (e WorkerNotFoundError) Error() string {
	return fmt.Sprintf("worker %s not found for queue %s", e.WorkerID, e.ResourcePath)
}

// JobExpiredError indicates a job visibility timeout expired
type JobExpiredError struct {
	JobID        string
	ResourcePath string
	ExpiredAt    time.Time
}

func (e JobExpiredError) Error() string {
	return fmt.Sprintf("job %s visibility timeout expired at %v in queue %s", e.JobID, e.ExpiredAt, e.ResourcePath)
}
