package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint represents a snapshot of the storage system state
type Checkpoint struct {
	// Version for compatibility
	Version int `json:"version"`

	// Timestamp when checkpoint was created
	Timestamp time.Time `json:"timestamp"`

	// Stream offsets per partition
	StreamOffsets map[string]map[int32]int64 `json:"stream_offsets"`

	// Consumer group offsets
	ConsumerGroupOffsets map[string]map[string]map[int32]int64 `json:"consumer_group_offsets"` // group -> stream -> partition -> offset

	// Queue metadata
	QueueMetadata map[string]*QueueCheckpoint `json:"queue_metadata"`

	// Replay session state
	ReplaySessions map[string]string `json:"replay_sessions"` // sessionID -> status
}

// QueueCheckpoint represents queue-specific checkpoint data
type QueueCheckpoint struct {
	ReadyCount    int64  `json:"ready_count"`
	InflightCount int64  `json:"inflight_count"`
	DLQCount      int64  `json:"dlq_count"`
	LastJobID     string `json:"last_job_id"`
}

// NewCheckpoint creates a new checkpoint with current timestamp
func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		Version:              1,
		Timestamp:            time.Now(),
		StreamOffsets:        make(map[string]map[int32]int64),
		ConsumerGroupOffsets: make(map[string]map[string]map[int32]int64),
		QueueMetadata:        make(map[string]*QueueCheckpoint),
		ReplaySessions:       make(map[string]string),
	}
}

// Save saves the checkpoint to a file atomically
func (c *Checkpoint) Save(path string) error {
	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temp file first (atomic write)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write checkpoint temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		//nolint:errcheck // Clean up temp file, ignore remove error
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	return nil
}

// Load loads a checkpoint from a file
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &checkpoint, nil
}
