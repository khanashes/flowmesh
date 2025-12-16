package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
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

	// Collect stream offsets
	// TODO: Add method to StreamManager to get all stream offsets
	// For now, leave empty - will be populated when we add recovery methods

	// Collect consumer group offsets
	// TODO: Add method to ConsumerGroupManager to get all group offsets

	// Collect queue metadata
	// TODO: Add method to QueueManager to get queue stats

	// Collect replay sessions
	// TODO: Add method to ReplayManager to get active sessions

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
