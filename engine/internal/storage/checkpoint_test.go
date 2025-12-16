package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "checkpoint.json")

	// Create checkpoint
	checkpoint := NewCheckpoint()
	checkpoint.StreamOffsets["stream1"] = map[int32]int64{0: 100, 1: 200}
	checkpoint.ConsumerGroupOffsets["group1"] = map[string]map[int32]int64{
		"stream1": {0: 50, 1: 150},
	}
	checkpoint.QueueMetadata["queue1"] = &QueueCheckpoint{
		ReadyCount:    10,
		InflightCount: 5,
		DLQCount:      2,
		LastJobID:     "job-123",
	}

	// Save checkpoint
	err := checkpoint.Save(checkpointPath)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(checkpointPath)
	require.NoError(t, err)

	// Load checkpoint
	loaded, err := LoadCheckpoint(checkpointPath)
	require.NoError(t, err)

	// Verify data
	assert.Equal(t, checkpoint.Version, loaded.Version)
	assert.Equal(t, checkpoint.StreamOffsets, loaded.StreamOffsets)
	assert.Equal(t, checkpoint.ConsumerGroupOffsets, loaded.ConsumerGroupOffsets)
	assert.Equal(t, checkpoint.QueueMetadata["queue1"].ReadyCount, loaded.QueueMetadata["queue1"].ReadyCount)
}

func TestCheckpoint_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "checkpoint.json")

	checkpoint := NewCheckpoint()
	checkpoint.StreamOffsets["stream1"] = map[int32]int64{0: 100}

	// Save checkpoint
	err := checkpoint.Save(checkpointPath)
	require.NoError(t, err)

	// Verify temp file was cleaned up
	tmpPath := checkpointPath + ".tmp"
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "Temp file should be cleaned up")
}

func TestCheckpointManager_Creation(t *testing.T) {
	tmpDir := t.TempDir()

	// Create minimal storage for testing
	storage := &Storage{
		paths: &StoragePaths{
			MetadataDir: tmpDir,
		},
	}

	manager := NewCheckpointManager(storage, tmpDir, 100*time.Millisecond)
	assert.NotNil(t, manager)
	assert.Equal(t, 100*time.Millisecond, manager.interval)
}

func TestCheckpointManager_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()

	// Create minimal storage for testing
	storage := &Storage{
		paths: &StoragePaths{
			MetadataDir: tmpDir,
		},
	}

	manager := NewCheckpointManager(storage, tmpDir, 1*time.Second)

	// Save a checkpoint
	err := manager.SaveCheckpoint()
	require.NoError(t, err)

	// Load the checkpoint
	checkpoint, err := manager.LoadLatestCheckpoint()
	require.NoError(t, err)
	assert.NotNil(t, checkpoint)
	assert.Equal(t, 1, checkpoint.Version)
}

func TestCheckpointManager_Rotation(t *testing.T) {
	tmpDir := t.TempDir()

	storage := &Storage{
		paths: &StoragePaths{
			MetadataDir: tmpDir,
		},
	}

	manager := NewCheckpointManager(storage, tmpDir, 1*time.Second)
	manager.maxCheckpoints = 3

	// Save multiple checkpoints
	for i := 0; i < 5; i++ {
		err := manager.SaveCheckpoint()
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Verify only 3 checkpoints remain
	checkpoints, err := manager.listCheckpoints()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(checkpoints), 3, "Should keep at most 3 checkpoints")
}

func TestCheckpointManager_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	storage := &Storage{
		paths: &StoragePaths{
			MetadataDir: tmpDir,
		},
	}

	manager := NewCheckpointManager(storage, tmpDir, 50*time.Millisecond)

	// Start manager
	manager.Start()

	// Wait for at least one checkpoint
	time.Sleep(100 * time.Millisecond)

	// Stop manager
	err := manager.Stop()
	require.NoError(t, err)

	// Verify at least one checkpoint was saved
	checkpoints, err := manager.listCheckpoints()
	require.NoError(t, err)
	assert.Greater(t, len(checkpoints), 0, "Should have saved at least one checkpoint")
}

func TestCheckpointManager_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	storage := &Storage{
		paths: &StoragePaths{
			MetadataDir: tmpDir,
		},
	}

	manager := NewCheckpointManager(storage, tmpDir, 1*time.Second)

	// Try to load checkpoint when none exists
	_, err := manager.LoadLatestCheckpoint()
	assert.Error(t, err, "Should error when no checkpoints exist")
}
