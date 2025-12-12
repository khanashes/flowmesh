package consumers

import "fmt"

// ConsumerGroupNotFoundError indicates a consumer group was not found
type ConsumerGroupNotFoundError struct {
	Stream string
	Group  string
}

func (e ConsumerGroupNotFoundError) Error() string {
	return fmt.Sprintf("consumer group '%s' not found for stream '%s'", e.Group, e.Stream)
}

// InvalidOffsetError indicates an invalid offset was provided
type InvalidOffsetError struct {
	Stream string
	Group  string
	Offset int64
	Reason string
}

func (e InvalidOffsetError) Error() string {
	return fmt.Sprintf("invalid offset %d for consumer group '%s' on stream '%s': %s", e.Offset, e.Group, e.Stream, e.Reason)
}

// CommitOffsetError indicates an error during offset commit
type CommitOffsetError struct {
	Stream string
	Group  string
	Err    error
}

func (e CommitOffsetError) Error() string {
	return fmt.Sprintf("failed to commit offset for consumer group '%s' on stream '%s': %v", e.Group, e.Stream, e.Err)
}

func (e CommitOffsetError) Unwrap() error {
	return e.Err
}

// GetOffsetError indicates an error retrieving offset
type GetOffsetError struct {
	Stream string
	Group  string
	Err    error
}

func (e GetOffsetError) Error() string {
	return fmt.Sprintf("failed to get offset for consumer group '%s' on stream '%s': %v", e.Group, e.Stream, e.Err)
}

func (e GetOffsetError) Unwrap() error {
	return e.Err
}

// CalculateLagError indicates an error calculating lag
type CalculateLagError struct {
	Stream string
	Group  string
	Err    error
}

func (e CalculateLagError) Error() string {
	return fmt.Sprintf("failed to calculate lag for consumer group '%s' on stream '%s': %v", e.Group, e.Stream, e.Err)
}

func (e CalculateLagError) Unwrap() error {
	return e.Err
}
