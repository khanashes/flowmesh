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
