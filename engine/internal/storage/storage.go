package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/kv"
	logpkg "github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/streams"
	"github.com/rs/zerolog"
)

// Storage represents the complete storage system
// It implements StorageBackend interface
type Storage struct {
	paths                *StoragePaths
	metaStore            *metastore.Store
	logManager           *logpkg.Manager
	streamManager        *streams.Manager
	queueManager         *queues.Manager
	kvManager            *kv.Manager
	consumerGroupManager *consumers.Manager
	log                  zerolog.Logger
	mu                   sync.RWMutex
	closed               bool
	ready                bool
}

// Ensure Storage implements StorageBackend interface
var _ StorageBackend = (*Storage)(nil)

// New creates a new storage system with default configuration
// For more control, use NewBuilder()
func New(dataDir string) (*Storage, error) {
	return NewBuilder().WithDataDir(dataDir).Build()
}

// MetaStore returns the metadata store
func (s *Storage) MetaStore() *metastore.Store {
	return s.metaStore
}

// LogManager returns the log manager
func (s *Storage) LogManager() LogManager {
	return s.logManager
}

// StreamManager returns the stream manager
func (s *Storage) StreamManager() StreamManager {
	return &streamManagerWrapper{Manager: s.streamManager}
}

// QueueManager returns the queue manager
func (s *Storage) QueueManager() QueueManager {
	return &queueManagerWrapper{Manager: s.queueManager}
}

// KVManager returns the KV store manager
func (s *Storage) KVManager() KVManager {
	return &kvManagerWrapper{Manager: s.kvManager}
}

// ConsumerGroupManager returns the consumer group manager
func (s *Storage) ConsumerGroupManager() ConsumerGroupManager {
	return &consumerGroupManagerWrapper{Manager: s.consumerGroupManager}
}

// Paths returns the storage paths
func (s *Storage) Paths() *StoragePaths {
	return s.paths
}

// Close gracefully shuts down the storage system
func (s *Storage) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.log.Info().Msg("Closing storage...")

	var lastErr error

	// Flush all segments
	if err := s.logManager.FlushAll(); err != nil {
		s.log.Error().Err(err).Msg("Failed to flush all segments")
		lastErr = err
	}

	// Close all segments
	if err := s.logManager.CloseAll(); err != nil {
		s.log.Error().Err(err).Msg("Failed to close all segments")
		lastErr = err
	}

	// Save stream indexes
	if err := s.streamManager.SaveIndexes(); err != nil {
		s.log.Error().Err(err).Msg("Failed to save stream indexes")
		lastErr = err
	}

	// Flush metadata store
	if err := s.metaStore.Flush(); err != nil {
		s.log.Error().Err(err).Msg("Failed to flush metadata store")
		lastErr = err
	}

	// Stop KV manager
	if err := s.kvManager.Stop(context.Background()); err != nil {
		s.log.Error().Err(err).Msg("Failed to stop KV manager")
		lastErr = err
	}

	// Stop consumer group manager
	if s.consumerGroupManager != nil {
		if err := s.consumerGroupManager.Stop(context.Background()); err != nil {
			s.log.Error().Err(err).Msg("Failed to stop consumer group manager")
			lastErr = err
		}
	}

	s.closed = true
	s.log.Info().Msg("Storage closed")

	return lastErr
}

// Start initializes and starts the storage system
func (s *Storage) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return nil
	}

	s.log.Info().Msg("Starting storage system...")

	// Validate storage
	if err := s.Validate(ctx); err != nil {
		return fmt.Errorf("storage validation failed: %w", err)
	}

	// Load metadata
	if err := s.metaStore.Load(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to load metadata: %w", err)
		}
		// Metadata file doesn't exist yet, that's okay
	}

	// Start KV manager
	if err := s.kvManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start KV manager: %w", err)
	}

	// Start consumer group manager
	if s.consumerGroupManager != nil {
		if err := s.consumerGroupManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start consumer group manager: %w", err)
		}
	}

	s.ready = true
	s.log.Info().Msg("Storage system started")

	return nil
}

// Stop gracefully stops the storage system
func (s *Storage) Stop(ctx context.Context) error {
	return s.Close(ctx)
}

// Ready returns true if the storage system is ready
func (s *Storage) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready && !s.closed
}

// Validate validates the storage system integrity
func (s *Storage) Validate(ctx context.Context) error {
	// Check that directories exist
	if err := validateStorageDirectory(s.paths.BaseDir); err != nil {
		return fmt.Errorf("base directory invalid: %w", err)
	}

	if err := validateStorageDirectory(s.paths.MetadataDir); err != nil {
		return fmt.Errorf("metadata directory invalid: %w", err)
	}

	// Try to load metadata
	if err := s.metaStore.Load(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to load metadata: %w", err)
		}
		// Metadata file doesn't exist yet, that's okay
	}

	return nil
}

// validateStorageDirectory checks if a directory exists and is accessible
func validateStorageDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}

	return nil
}
