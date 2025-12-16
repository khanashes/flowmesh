package replay

import (
	"fmt"
	"time"
)

// SessionNotFoundError indicates a replay session was not found
type SessionNotFoundError struct {
	SessionID string
}

func (e SessionNotFoundError) Error() string {
	return fmt.Sprintf("replay session not found: %s", e.SessionID)
}

// InvalidSessionStateError indicates an invalid session state transition
type InvalidSessionStateError struct {
	SessionID string
	Current   SessionStatus
	Requested SessionStatus
}

func (e InvalidSessionStateError) Error() string {
	return fmt.Sprintf("invalid session state transition: session %s is %s, cannot transition to %s", e.SessionID, e.Current, e.Requested)
}

// StreamNotFoundError indicates the stream for replay was not found
type StreamNotFoundError struct {
	Stream string
}

func (e StreamNotFoundError) Error() string {
	return fmt.Sprintf("stream not found: %s", e.Stream)
}

// InvalidOffsetError indicates an invalid offset was provided
type InvalidOffsetError struct {
	Offset int64
	Reason string
}

func (e InvalidOffsetError) Error() string {
	return fmt.Sprintf("invalid offset %d: %s", e.Offset, e.Reason)
}

// InvalidTimestampError indicates an invalid timestamp was provided
type InvalidTimestampError struct {
	Timestamp time.Time
	Reason    string
}

func (e InvalidTimestampError) Error() string {
	return fmt.Sprintf("invalid timestamp %v: %s", e.Timestamp, e.Reason)
}
