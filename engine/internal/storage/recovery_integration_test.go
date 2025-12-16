package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestConfig(dataDir string, policy string, interval time.Duration) *Config {
	config := DefaultConfig()
	config.DataDir = dataDir
	config.FsyncPolicy = policy
	config.FsyncInterval = interval
	return config
}

// TestRecovery_IntegrationEndToEnd tests the complete recovery flow
func TestRecovery_IntegrationEndToEnd(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("recovery after clean shutdown", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "clean-shutdown")

		// Phase 1: Create storage, write data, shutdown cleanly
		storage1, err := NewBuilder().
			WithDataDir(testDir).
			WithConfig(getTestConfig(testDir, "interval", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)

		// Create a stream and write data
		streamConfig := &metastore.ResourceConfig{
			Tenant:     "test",
			Namespace:  "ns",
			Name:       "stream1",
			Type:       metastore.ResourceStream,
			Partitions: 1,
		}
		require.NoError(t, streamConfig.Validate())
		require.NoError(t, storage1.metaStore.CreateResource(streamConfig))

		streamPath := streamConfig.GetPath()
		require.NoError(t, storage1.streamManager.InitializeStream(streamPath))

		// Write 100 events
		ctx := context.Background()
		for i := 0; i < 100; i++ {
			payload := []byte("event-data")
			_, err := storage1.streamManager.WriteEvents(ctx, streamPath, []streams.Event{{Payload: payload}})
			require.NoError(t, err)
		}

		// Clean shutdown
		require.NoError(t, storage1.Close(ctx))

		// Phase 2: Restart and verify data
		storage2, err := NewBuilder().
			WithDataDir(testDir).
			WithConfig(getTestConfig(testDir, "interval", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)
		defer storage2.Close(ctx)

		// Verify stream is accessible
		offset, err := storage2.streamManager.GetLatestOffset(streamPath, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(99), offset, "Should recover all 100 events (offset 0-99)")

		// Verify we can read data
		messages, err := storage2.streamManager.ReadFromOffset(ctx, streamPath, 0, 0, 10)
		require.NoError(t, err)
		assert.Len(t, messages, 10, "Should be able to read recovered events")
	})

	t.Run("recovery with corrupted segment", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "corrupted-segment")
		dataDir := filepath.Join(testDir, "data")

		// Create storage and write data
		storage1, err := NewBuilder().
			WithDataDir(dataDir).
			WithConfig(getTestConfig(dataDir, "always", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)

		// Create and write to stream
		streamConfig := &metastore.ResourceConfig{
			Tenant:     "test",
			Namespace:  "ns",
			Name:       "stream1",
			Type:       metastore.ResourceStream,
			Partitions: 1,
		}
		require.NoError(t, streamConfig.Validate())
		require.NoError(t, storage1.metaStore.CreateResource(streamConfig))

		streamPath := streamConfig.GetPath()
		require.NoError(t, storage1.streamManager.InitializeStream(streamPath))

		ctx := context.Background()
		for i := 0; i < 50; i++ {
			_, err := storage1.streamManager.WriteEvents(ctx, streamPath, []streams.Event{{Payload: []byte("test")}})
			require.NoError(t, err)
		}

		require.NoError(t, storage1.Close(ctx))

		// Corrupt a segment file by appending garbage
		// dataDir is already defined above

		// Use nested glob to find segments in hashed directories
		segments, err := filepath.Glob(filepath.Join(dataDir, "*", "segment-*.log"))
		require.NoError(t, err)
		require.Greater(t, len(segments), 0, "Should have at least one segment")

		// Append corrupted data to first segment
		f, err := os.OpenFile(segments[0], os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte("CORRUPTED_DATA_THAT_WILL_FAIL_CRC"))
		require.NoError(t, err)
		f.Close()

		// Restart and verify recovery handles corruption
		storage2, err := NewBuilder().
			WithDataDir(dataDir).
			WithConfig(getTestConfig(dataDir, "interval", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)
		defer storage2.Close(ctx)

		// Should still be able to read valid data
		messages, err := storage2.streamManager.ReadFromOffset(ctx, streamPath, 0, 0, 10)
		require.NoError(t, err)
		assert.Greater(t, len(messages), 0, "Should recover non-corrupted events")
	})

	t.Run("checkpoint save and load", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "checkpoint-test")

		storage1, err := NewBuilder().
			WithDataDir(testDir).
			WithConfig(getTestConfig(testDir, "interval", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)

		// Wait for at least one checkpoint to be saved
		time.Sleep(DefaultCheckpointInterval + 100*time.Millisecond)

		// Close storage
		require.NoError(t, storage1.Close(context.Background()))

		// Verify checkpoint files exist
		checkpointDir := filepath.Join(testDir, "metadata")
		checkpoints, err := filepath.Glob(filepath.Join(checkpointDir, "checkpoint-*.json"))
		require.NoError(t, err)
		assert.Greater(t, len(checkpoints), 0, "Should have created checkpoint files")

		// Restart and verify checkpoint is loaded
		storage2, err := NewBuilder().
			WithDataDir(testDir).
			WithConfig(getTestConfig(testDir, "interval", 10*time.Millisecond)).
			BuildAndStart(context.Background())
		require.NoError(t, err)
		defer storage2.Close(context.Background())

		// Recovery should have loaded the checkpoint without errors
		assert.True(t, storage2.Ready(), "Storage should be ready after checkpoint recovery")
	})

	t.Run("log manager recovery", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "log-recovery")

		// Create log manager and write data
		logMgr1 := log.NewManager(filepath.Join(testDir, "logs"), log.FsyncAlways, 10*time.Millisecond)

		writer, err := logMgr1.OpenSegment("test/resource", 0)
		require.NoError(t, err)

		// Write some entries
		for i := 0; i < 20; i++ {
			err := writer.WriteEntry([]byte("log entry data"))
			require.NoError(t, err)
		}

		require.NoError(t, logMgr1.Shutdown())

		// Create new log manager and recover
		logMgr2 := log.NewManager(filepath.Join(testDir, "logs"), log.FsyncInterval, 10*time.Millisecond)
		err = logMgr2.Recover()
		require.NoError(t, err, "Recovery should succeed")

		// Verify we can still access the resource
		writer2, err := logMgr2.OpenSegment("test/resource", 0)
		require.NoError(t, err)

		// Should be able to write more data
		err = writer2.WriteEntry([]byte("new entry after recovery"))
		require.NoError(t, err)

		require.NoError(t, logMgr2.Shutdown())
	})
}

// TestRecovery_Queue tests queue recovery
func TestRecovery_Queue(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Phase 1: Create queue, enqueue jobs, ACK some
	storage1, err := NewBuilder().
		WithDataDir(dataDir).
		WithConfig(getTestConfig(dataDir, "always", 10*time.Millisecond)).
		BuildAndStart(context.Background())
	require.NoError(t, err)

	queueConfig := &metastore.ResourceConfig{
		Tenant:     "test",
		Namespace:  "ns",
		Name:       "queue1",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	require.NoError(t, queueConfig.Validate())
	require.NoError(t, storage1.metaStore.CreateResource(queueConfig))
	queuePath := queueConfig.GetPath()

	// Enqueue Job 1
	job1, _, err := storage1.queueManager.Enqueue(context.Background(), queuePath, []byte("job1"), queues.DefaultEnqueueOptions())
	require.NoError(t, err)

	// Enqueue Job 2
	job2, _, err := storage1.queueManager.Enqueue(context.Background(), queuePath, []byte("job2"), queues.DefaultEnqueueOptions())
	require.NoError(t, err)

	// Reserve and ACK Job 1
	reserved, err := storage1.queueManager.AddToInFlight(queuePath, job1, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, reserved)
	err = storage1.queueManager.ACK(context.Background(), queuePath, job1)
	require.NoError(t, err)

	// Close storage
	require.NoError(t, storage1.Close(context.Background()))

	// Phase 2: Restart and verify
	storage2, err := NewBuilder().
		WithDataDir(dataDir).
		WithConfig(getTestConfig(dataDir, "always", 10*time.Millisecond)).
		BuildAndStart(context.Background())
	require.NoError(t, err)
	defer storage2.Close(context.Background())

	// Verify Job 1 is GONE (ACKed)
	// We check if we can reserve it again (should fail)
	// Or check ReadyHeap count if exposed, but we rely on behavior

	// Attempt to reserve Job 2
	reserved2, err := storage2.queueManager.AddToInFlight(queuePath, job2, 30*time.Second)
	require.NoError(t, err, "Job 2 should be recoverable")
	assert.Equal(t, job2, reserved2.ID)

	// Attempt to reserve Job 1 should fail
	_, err = storage2.queueManager.AddToInFlight(queuePath, job1, 30*time.Second)
	assert.Error(t, err, "Job 1 should be ACKed and gone")
}

// TestRecovery_FsyncPolicies tests different fsync policies
func TestRecovery_FsyncPolicies(t *testing.T) {
	tmpDir := t.TempDir()

	policies := []struct {
		name     string
		policy   string
		interval time.Duration
	}{
		{"always", "always", 10 * time.Millisecond},
		{"interval-10ms", "interval", 10 * time.Millisecond},
		{"interval-50ms", "interval", 50 * time.Millisecond},
	}

	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			testDir := filepath.Join(tmpDir, tc.name)

			storage, err := NewBuilder().
				WithDataDir(testDir).
				WithConfig(getTestConfig(testDir, tc.policy, tc.interval)).
				BuildAndStart(context.Background())
			require.NoError(t, err)

			// Create stream and write data
			streamConfig := &metastore.ResourceConfig{
				Tenant:     "test",
				Namespace:  "ns",
				Name:       "stream1",
				Type:       metastore.ResourceStream,
				Partitions: 1,
			}
			require.NoError(t, streamConfig.Validate())
			require.NoError(t, storage.metaStore.CreateResource(streamConfig))

			streamPath := streamConfig.GetPath()
			require.NoError(t, storage.streamManager.InitializeStream(streamPath))

			ctx := context.Background()
			for i := 0; i < 100; i++ {
				_, err := storage.streamManager.WriteEvents(ctx, streamPath, []streams.Event{{Payload: []byte("data")}})
				require.NoError(t, err)
			}

			// For interval policy, wait for fsync
			if tc.policy == "interval" {
				time.Sleep(tc.interval * 2)
			}

			require.NoError(t, storage.Close(ctx))

			// Verify data persisted
			storage2, err := NewBuilder().
				WithDataDir(testDir).
				WithConfig(getTestConfig(testDir, tc.policy, tc.interval)).
				BuildAndStart(context.Background())
			require.NoError(t, err)
			defer storage2.Close(ctx)

			offset, err := storage2.streamManager.GetLatestOffset(streamPath, 0)
			require.NoError(t, err)
			assert.Equal(t, int64(99), offset, "All events should be persisted")
		})
	}
}
