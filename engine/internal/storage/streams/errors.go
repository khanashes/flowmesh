package streams

import "fmt"

// StreamNotFoundError indicates a stream resource was not found
type StreamNotFoundError struct {
	ResourcePath string
}

func (e StreamNotFoundError) Error() string {
	return fmt.Sprintf("stream not found: %s", e.ResourcePath)
}

// InvalidOffsetError indicates an invalid offset was provided
type InvalidOffsetError struct {
	Offset       int64
	ResourcePath string
	Reason       string
}

func (e InvalidOffsetError) Error() string {
	return fmt.Sprintf("invalid offset %d for stream %s: %s", e.Offset, e.ResourcePath, e.Reason)
}

// ReadError indicates an error reading from a stream
type ReadError struct {
	ResourcePath string
	Offset       int64
	Err          error
}

func (e ReadError) Error() string {
	return fmt.Sprintf("failed to read from stream %s at offset %d: %v", e.ResourcePath, e.Offset, e.Err)
}

func (e ReadError) Unwrap() error {
	return e.Err
}

// WriteError indicates an error writing to a stream
type WriteError struct {
	ResourcePath string
	Err          error
}

func (e WriteError) Error() string {
	return fmt.Sprintf("failed to write to stream %s: %v", e.ResourcePath, e.Err)
}

func (e WriteError) Unwrap() error {
	return e.Err
}
