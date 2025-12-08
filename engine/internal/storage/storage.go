package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/flowmesh/engine/internal/logger"
	logpkg "github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/streams"
	"github.com/rs/zerolog"
)

// Storage represents the complete storage system
type Storage struct {
	paths         *StoragePaths
	metaStore     *metastore.Store
	logManager    *logpkg.Manager
	streamManager *streams.Manager
	queueManager  *queues.Manager
	log           zerolog.Logger
	mu            sync.RWMutex
	closed        bool
}

// New creates a new storage system
func New(dataDir string) (*Storage, error) {
	log := logger.WithComponent("storage")

	// Initialize directories
	paths, err := InitDirectories(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize directories: %w", err)
	}

	// Initialize metadata store
	metaStore, err := metastore.NewStore(paths.MetadataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// Initialize log manager
	logManager := logpkg.NewManager(paths.BaseDir)

	storage := &Storage{
		paths:      paths,
		metaStore:  metaStore,
		logManager: logManager,
		log:        log,
		closed:     false,
	}

	// Initialize stream manager
	storage.streamManager = streams.NewManager(metaStore, logManager, paths.MetadataDir)

	// Initialize queue manager
	storage.queueManager = queues.NewManager(metaStore, logManager, paths.MetadataDir)

	log.Info().
		Str("data_dir", dataDir).
		Msg("Storage initialized")

	return storage, nil
}

// MetaStore returns the metadata store
func (s *Storage) MetaStore() *metastore.Store {
	return s.metaStore
}

// LogManager returns the log manager
func (s *Storage) LogManager() *logpkg.Manager {
	return s.logManager
}

// StreamManager returns the stream manager
func (s *Storage) StreamManager() *streams.Manager {
	return s.streamManager
}

// QueueManager returns the queue manager
func (s *Storage) QueueManager() *queues.Manager {
	return s.queueManager
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

	s.closed = true
	s.log.Info().Msg("Storage closed")

	return lastErr
}

// Validate validates the storage system integrity
func (s *Storage) Validate() error {
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
