package storage

import (
	"time"
)

// Config holds configuration for the storage system
type Config struct {
	// DataDir is the base directory for storage
	DataDir string

	// SegmentMaxSize is the maximum size of a log segment before rotation (bytes)
	SegmentMaxSize int64

	// SegmentMaxAge is the maximum age of a log segment before rotation
	SegmentMaxAge time.Duration

	// FlushInterval is the interval for flushing segments to disk
	FlushInterval time.Duration

	// FsyncPolicy determines when to sync data to disk ("always", "interval")
	FsyncPolicy string

	// FsyncInterval is the interval for interval-based fsyncing
	FsyncInterval time.Duration

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// EnableTracing enables distributed tracing
	EnableTracing bool
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		DataDir:        "./data",
		SegmentMaxSize: 100 * 1024 * 1024, // 100MB
		SegmentMaxAge:  24 * time.Hour,
		FlushInterval:  5 * time.Second,
		FsyncPolicy:    "interval",
		FsyncInterval:  10 * time.Millisecond,
		EnableMetrics:  true,
		EnableTracing:  false,
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return ErrInvalidConfig{Field: "DataDir", Reason: "cannot be empty"}
	}
	if c.SegmentMaxSize <= 0 {
		return ErrInvalidConfig{Field: "SegmentMaxSize", Reason: "must be greater than zero"}
	}
	if c.SegmentMaxAge <= 0 {
		return ErrInvalidConfig{Field: "SegmentMaxAge", Reason: "must be greater than zero"}
	}
	if c.FlushInterval <= 0 {
		return ErrInvalidConfig{Field: "FlushInterval", Reason: "must be greater than zero"}
	}
	if c.FsyncPolicy != "always" && c.FsyncPolicy != "interval" {
		return ErrInvalidConfig{Field: "FsyncPolicy", Reason: "must be 'always' or 'interval'"}
	}
	if c.FsyncPolicy == "interval" && c.FsyncInterval <= 0 {
		return ErrInvalidConfig{Field: "FsyncInterval", Reason: "must be greater than zero for interval policy"}
	}
	return nil
}

// ErrInvalidConfig indicates an invalid configuration
type ErrInvalidConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidConfig) Error() string {
	return "invalid config: " + e.Field + ": " + e.Reason
}
