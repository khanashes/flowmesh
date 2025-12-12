package kv

import (
	"time"
)

// Value represents a stored value with metadata
type Value struct {
	// Payload is the actual data stored
	Payload []byte
	// ExpiresAt is the expiration time (nil = no expiration)
	ExpiresAt *time.Time
	// CreatedAt is when the value was created
	CreatedAt time.Time
}

// SetOptions specifies options for Set operations
type SetOptions struct {
	// TTL is the time-to-live duration (0 = no expiration)
	TTL time.Duration
}

// DefaultSetOptions returns default set options
func DefaultSetOptions() SetOptions {
	return SetOptions{
		TTL: 0, // No expiration by default
	}
}
