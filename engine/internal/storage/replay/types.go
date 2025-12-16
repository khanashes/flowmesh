package replay

import (
	"time"
)

// SessionStatus represents the status of a replay session
type SessionStatus string

const (
	// SessionStatusCreated indicates the session was created but not started
	SessionStatusCreated SessionStatus = "created"
	// SessionStatusActive indicates the session is actively replaying
	SessionStatusActive SessionStatus = "active"
	// SessionStatusPaused indicates the session is paused
	SessionStatusPaused SessionStatus = "paused"
	// SessionStatusStopped indicates the session was stopped
	SessionStatusStopped SessionStatus = "stopped"
	// SessionStatusCompleted indicates the session completed successfully
	SessionStatusCompleted SessionStatus = "completed"
	// SessionStatusError indicates an error occurred
	SessionStatusError SessionStatus = "error"
)

// Session represents a replay session
type Session struct {
	ID                   string
	Stream               string
	Partition            int32
	StartOffset          int64
	StartTime            *time.Time
	EndOffset            *int64
	EndTime              *time.Time
	SandboxConsumerGroup string
	Status               SessionStatus
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// Progress represents the progress of a replay session
type Progress struct {
	CurrentOffset    int64
	MessagesReplayed int64
	Errors           int64
	StartedAt        *time.Time
	PausedAt         *time.Time
	CompletedAt      *time.Time
}

// StartRequest represents a request to start a replay session
type StartRequest struct {
	Stream               string
	Partition            int32
	StartOffset          *int64
	StartTime            *time.Time
	EndOffset            *int64
	EndTime              *time.Time
	SandboxConsumerGroup string
}
