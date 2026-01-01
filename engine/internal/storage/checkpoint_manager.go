package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

const (
	// DefaultCheckpointInterval is how often to save checkpoints
	DefaultCheckpointInterval = 30 * time.Second
	// DefaultMaxCheckpoints is how many checkpoint files to keep
	DefaultMaxCheckpoints = 5
	// CheckpointFilePattern is the filename pattern for checkpoints
	CheckpointFilePattern = "checkpoint-%d.json"
)

// CheckpointManager manages periodic checkpointing of storage state
type CheckpointManager struct {
	storage          *Storage
	checkpointDir    string
	interval         time.Duration
	maxCheckpoints   int
	stopCh           chan struct{}
	wg               sync.WaitGroup
	log              zerolog.Logger
	lastCheckpointAt time.Time
	mu               sync.RWMutex
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(storage *Storage, checkpointDir string, interval time.Duration) *CheckpointManager {
	return &CheckpointManager{
		storage:        storage,
		checkpointDir:  checkpointDir,
		interval:       interval,
		maxCheckpoints: DefaultMaxCheckpoints,
		stopCh:         make(chan struct{}),
		log:            logger.WithComponent("checkpoint"),
	}
}

// Start starts the checkpoint manager
func (cm *CheckpointManager) Start() {
	cm.wg.Add(1)
	go cm.run()
	cm.log.Info().Dur("interval", cm.interval).Msg("Checkpoint manager started")
}

// Stop stops the checkpoint manager
func (cm *CheckpointManager) Stop() error {
	close(cm.stopCh)
	cm.wg.Wait()

	// Save final checkpoint before shutdown
	if err := cm.SaveCheckpoint(); err != nil {
		return fmt.Errorf("failed to save final checkpoint: %w", err)
	}

	cm.log.Info().Msg("Checkpoint manager stopped")
	return nil
}

// run executes the checkpoint loop
func (cm *CheckpointManager) run() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			if err := cm.SaveCheckpoint(); err != nil {
				cm.log.Error().Err(err).Msg("Failed to save checkpoint")
			}
		}
	}
}

// SaveCheckpoint saves the current storage state to a checkpoint file
func (cm *CheckpointManager) SaveCheckpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	start := time.Now()

	// Collect state from all managers
	checkpoint := cm.collectState()

	// Generate checkpoint file path
	checkpointPath := cm.getCheckpointPath(checkpoint.Timestamp)

	// Save checkpoint
	if err := checkpoint.Save(checkpointPath); err != nil {
		return err
	}

	// Rotate old checkpoints
	if err := cm.rotateCheckpoints(); err != nil {
		cm.log.Warn().Err(err).Msg("Failed to rotate old checkpoints")
	}

	cm.lastCheckpointAt = checkpoint.Timestamp
	duration := time.Since(start)

	cm.log.Info().
		Str("path", checkpointPath).
		Dur("duration", duration).
		Msg("Checkpoint saved successfully")

	return nil
}

// collectState collects state from all storage managers
func (cm *CheckpointManager) collectState() *Checkpoint {
	checkpoint := NewCheckpoint()
	ctx := context.Background()

	// Check if metaStore is available
	if cm.storage.metaStore == nil {
		return checkpoint
	}

	// Collect stream offsets
	streamResources, err := cm.storage.metaStore.ListResources("", "", metastore.ResourceStream)
	if err == nil {
		streamMgr := cm.storage.StreamManager()
		for _, resource := range streamResources {
			resourcePath := resource.GetPath()
			// Get latest offset for partition 0 (MVP)
			partition := int32(0)
			offset, err := streamMgr.GetLatestOffset(ctx, resourcePath, partition)
			if err == nil && offset >= 0 {
				if checkpoint.StreamOffsets[resourcePath] == nil {
					checkpoint.StreamOffsets[resourcePath] = make(map[int32]int64)
				}
				checkpoint.StreamOffsets[resourcePath][partition] = offset
			}
		}
	} else {
		cm.log.Warn().Err(err).Msg("Failed to list streams for checkpoint")
	}

	// Collect consumer group offsets
	consumerMgr := cm.storage.ConsumerGroupManager()
	streamResources, err = cm.storage.metaStore.ListResources("", "", metastore.ResourceStream)
	if err == nil {
		for _, resource := range streamResources {
			resourcePath := resource.GetPath()
			// List all consumer groups for this stream
			groups, err := consumerMgr.ListConsumerGroups(ctx, resourcePath)
			if err != nil {
				continue
			}

			// Get offset for each consumer group
			for _, group := range groups {
				partition := int32(0)
				offset, err := consumerMgr.GetCommittedOffset(ctx, resourcePath, group, partition)
				if err == nil {
					if checkpoint.ConsumerGroupOffsets[group] == nil {
						checkpoint.ConsumerGroupOffsets[group] = make(map[string]map[int32]int64)
					}
					if checkpoint.ConsumerGroupOffsets[group][resourcePath] == nil {
						checkpoint.ConsumerGroupOffsets[group][resourcePath] = make(map[int32]int64)
					}
					checkpoint.ConsumerGroupOffsets[group][resourcePath][partition] = offset
				}
			}
		}
	} else {
		cm.log.Warn().Err(err).Msg("Failed to list streams for consumer group checkpoint")
	}

	// Collect queue metadata
	queueResources, err := cm.storage.metaStore.ListResources("", "", metastore.ResourceQueue)
	if err == nil {
		queueMgr := cm.storage.QueueManager()
		for _, resource := range queueResources {
			resourcePath := resource.GetPath()
			stats, err := queueMgr.GetQueueStats(ctx, resourcePath)
			if err == nil {
				checkpoint.QueueMetadata[resourcePath] = &QueueCheckpoint{
					ReadyCount:    stats.PendingJobs,
					InflightCount: stats.InFlightJobs,
					DLQCount:      0,  // DLQ count would need separate tracking
					LastJobID:     "", // Last job ID would need separate tracking
				}
			}
		}
	} else {
		cm.log.Warn().Err(err).Msg("Failed to list queues for checkpoint")
	}

	// Collect replay sessions
	replayMgr := cm.storage.ReplayManager()
	sessions, err := replayMgr.ListSessions(ctx, "")
	if err == nil {
		for _, session := range sessions {
			checkpoint.ReplaySessions[session.ID] = session.Status
		}
	} else {
		cm.log.Warn().Err(err).Msg("Failed to list replay sessions for checkpoint")
	}

	return checkpoint
}

// LoadLatestCheckpoint loads the most recent checkpoint
func (cm *CheckpointManager) LoadLatestCheckpoint() (*Checkpoint, error) {
	checkpoints, err := cm.listCheckpoints()
	if err != nil {
		return nil, err
	}

	if len(checkpoints) == 0 {
		return nil, fmt.Errorf("no checkpoints found")
	}

	// Load the latest checkpoint
	latestPath := checkpoints[len(checkpoints)-1]
	checkpoint, err := LoadCheckpoint(latestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint %s: %w", latestPath, err)
	}

	cm.log.Info().
		Str("path", latestPath).
		Time("timestamp", checkpoint.Timestamp).
		Msg("Loaded checkpoint")

	return checkpoint, nil
}

// getCheckpointPath generates a checkpoint file path
func (cm *CheckpointManager) getCheckpointPath(timestamp time.Time) string {
	filename := fmt.Sprintf(CheckpointFilePattern, timestamp.Unix())
	return filepath.Join(cm.checkpointDir, filename)
}

// listCheckpoints lists all checkpoint files sorted by timestamp
func (cm *CheckpointManager) listCheckpoints() ([]string, error) {
	pattern := filepath.Join(cm.checkpointDir, "checkpoint-*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	// Sort by filename (which includes timestamp)
	sort.Strings(files)
	return files, nil
}

// rotateCheckpoints removes old checkpoint files
func (cm *CheckpointManager) rotateCheckpoints() error {
	checkpoints, err := cm.listCheckpoints()
	if err != nil {
		return err
	}

	// Keep only the latest N checkpoints
	if len(checkpoints) > cm.maxCheckpoints {
		toDelete := checkpoints[:len(checkpoints)-cm.maxCheckpoints]
		for _, path := range toDelete {
			if err := os.Remove(path); err != nil {
				cm.log.Warn().Err(err).Str("path", path).Msg("Failed to delete old checkpoint")
			} else {
				cm.log.Debug().Str("path", path).Msg("Deleted old checkpoint")
			}
		}
	}

	return nil
}

// GetLastCheckpointTime returns the timestamp of the last checkpoint
func (cm *CheckpointManager) GetLastCheckpointTime() time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.lastCheckpointAt
}
