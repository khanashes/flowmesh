package storage

import (
	"context"
	"fmt"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/kv"
	logpkg "github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/streams"
	"github.com/rs/zerolog"
)

// Builder provides a fluent interface for building Storage instances
type Builder struct {
	config      *Config
	metaStore   *metastore.Store
	logManager  *logpkg.Manager
	streamMgr   *streams.Manager
	queueMgr    *queues.Manager
	log         zerolog.Logger
	initialized bool
}

// NewBuilder creates a new Storage builder
func NewBuilder() *Builder {
	return &Builder{
		config: DefaultConfig(),
		log:    logger.WithComponent("storage.builder"),
	}
}

// WithConfig sets the configuration
func (b *Builder) WithConfig(config *Config) *Builder {
	b.config = config
	return b
}

// WithDataDir sets the data directory (convenience method)
func (b *Builder) WithDataDir(dataDir string) *Builder {
	if b.config == nil {
		b.config = DefaultConfig()
	}
	b.config.DataDir = dataDir
	return b
}

// WithMetaStore sets a custom metadata store (optional, will create default if not set)
func (b *Builder) WithMetaStore(metaStore *metastore.Store) *Builder {
	b.metaStore = metaStore
	return b
}

// WithLogManager sets a custom log manager (optional, will create default if not set)
func (b *Builder) WithLogManager(logManager *logpkg.Manager) *Builder {
	b.logManager = logManager
	return b
}

// WithStreamManager sets a custom stream manager (optional, will create default if not set)
func (b *Builder) WithStreamManager(streamMgr *streams.Manager) *Builder {
	b.streamMgr = streamMgr
	return b
}

// WithQueueManager sets a custom queue manager (optional, will create default if not set)
func (b *Builder) WithQueueManager(queueMgr *queues.Manager) *Builder {
	b.queueMgr = queueMgr
	return b
}

// Build creates and initializes the Storage instance
func (b *Builder) Build() (*Storage, error) {
	if b.config == nil {
		b.config = DefaultConfig()
	}

	if err := b.config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Initialize directories
	paths, err := InitDirectories(b.config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize directories: %w", err)
	}

	// Initialize metadata store if not provided
	if b.metaStore == nil {
		metaStore, err := metastore.NewStore(paths.MetadataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create metadata store: %w", err)
		}
		b.metaStore = metaStore
	}

	// Initialize log manager if not provided
	if b.logManager == nil {
		b.logManager = logpkg.NewManager(paths.BaseDir)
	}

	// Initialize stream manager if not provided
	var streamMgr *streams.Manager
	if b.streamMgr == nil {
		streamMgr = streams.NewManager(b.metaStore, b.logManager, paths.MetadataDir)
	} else {
		streamMgr = b.streamMgr
	}

	// Initialize queue manager if not provided
	var queueMgr *queues.Manager
	if b.queueMgr == nil {
		queueMgr = queues.NewManager(b.metaStore, b.logManager, paths.MetadataDir)
	} else {
		queueMgr = b.queueMgr
	}

	// Initialize KV manager
	kvMgr := kv.NewManager(b.metaStore, paths.KVDir)

	// Initialize consumer group manager with a function to get latest offset from stream manager
	// This closure captures the streamMgr to avoid circular dependencies
	latestOffsetFunc := func(stream string, partition int32) (int64, error) {
		return streamMgr.GetLatestOffset(stream, partition)
	}
	consumerGroupMgr := consumers.NewManager(b.metaStore, paths.MetadataDir, latestOffsetFunc)

	storage := &Storage{
		paths:                paths,
		metaStore:            b.metaStore,
		logManager:           b.logManager,
		streamManager:        streamMgr,
		queueManager:         queueMgr,
		kvManager:            kvMgr,
		consumerGroupManager: consumerGroupMgr,
		log:                  b.log,
		closed:               false,
		ready:                false,
	}

	b.log.Info().
		Str("data_dir", b.config.DataDir).
		Msg("Storage built successfully")

	return storage, nil
}

// BuildAndStart creates, initializes, and starts the Storage instance
func (b *Builder) BuildAndStart(ctx context.Context) (*Storage, error) {
	storage, err := b.Build()
	if err != nil {
		return nil, err
	}

	if err := storage.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start storage: %w", err)
	}

	return storage, nil
}
