package storage

import (
	"context"
	"fmt"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/metrics"
	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/kv"
	logpkg "github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/replay"
	"github.com/flowmesh/engine/internal/storage/schema"
	"github.com/flowmesh/engine/internal/storage/streams"
	"github.com/flowmesh/engine/internal/tracing"
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
		// Parse fsync policy
		fsyncPolicy := logpkg.FsyncPolicy(b.config.FsyncPolicy)
		b.logManager = logpkg.NewManager(paths.BaseDir, fsyncPolicy, b.config.FsyncInterval)
	}

	// Initialize metrics if enabled
	var metricsCollector *metrics.Collector
	var queueMetrics *metrics.QueueMetrics
	var streamMetrics *metrics.StreamMetrics
	var consumerMetrics *metrics.ConsumerGroupMetrics
	var nodeMetrics *metrics.NodeMetrics

	if b.config.EnableMetrics {
		metricsCollector = metrics.NewCollector()
		queueMetrics = metrics.NewQueueMetrics(metricsCollector)
		streamMetrics = metrics.NewStreamMetrics(metricsCollector)
		consumerMetrics = metrics.NewConsumerGroupMetrics(metricsCollector)
		nodeMetrics = metrics.NewNodeMetrics(metricsCollector)
	}

	// Initialize tracing if enabled
	var tracerProvider *tracing.Provider
	if b.config.EnableTracing {
		tracingConfig := tracing.DefaultTracingConfig()
		tracingConfig.Enabled = true
		// Endpoint will be set from higher-level config if available
		// Sampling config uses defaults from DefaultTracingConfig (always sample)
		var err error
		tracerProvider, err = tracing.NewProvider(tracingConfig)
		if err != nil {
			b.log.Warn().Err(err).Msg("Failed to initialize tracing provider, continuing without tracing")
			tracerProvider = nil
		}
	}

	// Initialize stream manager if not provided
	var streamMgr *streams.Manager
	if b.streamMgr == nil {
		streamMgr = streams.NewManager(b.metaStore, b.logManager, paths.MetadataDir, streamMetrics)
	} else {
		streamMgr = b.streamMgr
	}

	// Initialize queue manager if not provided
	var queueMgr *queues.Manager
	if b.queueMgr == nil {
		queueMgr = queues.NewManager(b.metaStore, b.logManager, paths.MetadataDir, queueMetrics)
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
	consumerGroupMgr := consumers.NewManager(b.metaStore, paths.MetadataDir, latestOffsetFunc, consumerMetrics)

	// Initialize schema registry
	schemaReg := schema.NewRegistry(b.metaStore)

	// Initialize replay executor
	replayExecutor := replay.NewExecutor(nil, streamMgr, consumerGroupMgr) // Manager will be set after creation

	// Initialize replay manager
	replayMgr := replay.NewManager(b.metaStore, paths.MetadataDir, replayExecutor)
	replayExecutor.SetManager(replayMgr) // Set manager reference

	storage := &Storage{
		paths:                paths,
		metaStore:            b.metaStore,
		logManager:           b.logManager,
		streamManager:        streamMgr,
		queueManager:         queueMgr,
		kvManager:            kvMgr,
		consumerGroupManager: consumerGroupMgr,
		schemaRegistry:       schemaReg,
		replayManager:        replayMgr,
		metricsCollector:     metricsCollector,
		nodeMetrics:          nodeMetrics,
		tracerProvider:       tracerProvider,
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
