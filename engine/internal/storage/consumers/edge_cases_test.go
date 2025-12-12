package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommitOffset_EdgeCases tests edge cases for CommitOffset
func TestCommitOffset_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	t.Run("Commit same offset multiple times (idempotency)", func(t *testing.T) {
		offset := int64(42)

		// Commit first time
		err := manager.CommitOffset(ctx, stream, group, partition, offset)
		require.NoError(t, err)

		// Commit same offset again
		err = manager.CommitOffset(ctx, stream, group, partition, offset)
		assert.NoError(t, err)

		// Verify offset is still correct
		committed, err := manager.GetCommittedOffset(ctx, stream, group, partition)
		require.NoError(t, err)
		assert.Equal(t, offset, committed)
	})

	t.Run("Commit offset -1 (reset)", func(t *testing.T) {
		// First commit a valid offset
		err := manager.CommitOffset(ctx, stream, group, partition, 100)
		require.NoError(t, err)

		// Commit -1 to reset
		err = manager.CommitOffset(ctx, stream, group, partition, -1)
		require.NoError(t, err)

		// Verify offset is -1
		committed, err := manager.GetCommittedOffset(ctx, stream, group, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), committed)
	})

	t.Run("Commit very large offset", func(t *testing.T) {
		largeOffset := int64(9223372036854775807) // Max int64
		err := manager.CommitOffset(ctx, stream, group, partition, largeOffset)
		require.NoError(t, err)

		committed, err := manager.GetCommittedOffset(ctx, stream, group, partition)
		require.NoError(t, err)
		assert.Equal(t, largeOffset, committed)
	})

	t.Run("Commit offset with context cancellation", func(t *testing.T) {
		// Note: Manager doesn't check context at start, wrapper does
		// This test verifies the method still works even with cancelled context
		// (context is only used for stream validation via MetaStore)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// This will work because MetaStore validation doesn't check context
		// In real usage, wrapper would catch context cancellation
		err := manager.CommitOffset(ctx, stream, group, partition, 10)
		assert.NoError(t, err)
	})

	t.Run("Commit offset with empty consumer group name", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, "", partition, 10)
		require.NoError(t, err) // Should allow empty group name

		committed, err := manager.GetCommittedOffset(ctx, stream, "", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(10), committed)
	})

	t.Run("Commit offset with unicode consumer group name", func(t *testing.T) {
		unicodeGroup := "测试组-🧪-グループ"
		err := manager.CommitOffset(ctx, stream, unicodeGroup, partition, 20)
		require.NoError(t, err)

		committed, err := manager.GetCommittedOffset(ctx, stream, unicodeGroup, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(20), committed)
	})

	t.Run("Commit offset with special characters in group name", func(t *testing.T) {
		specialGroup := "group-with-special-chars-|*&^%$#@!"
		err := manager.CommitOffset(ctx, stream, specialGroup, partition, 30)
		require.NoError(t, err)

		committed, err := manager.GetCommittedOffset(ctx, stream, specialGroup, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(30), committed)
	})

	t.Run("Commit offset for multiple partitions", func(t *testing.T) {
		// Commit for partition 0
		err := manager.CommitOffset(ctx, stream, group, 0, 100)
		require.NoError(t, err)

		// Commit for partition 1
		err = manager.CommitOffset(ctx, stream, group, 1, 200)
		require.NoError(t, err)

		// Verify both offsets are stored separately
		offset0, err := manager.GetCommittedOffset(ctx, stream, group, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(100), offset0)

		offset1, err := manager.GetCommittedOffset(ctx, stream, group, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(200), offset1)
	})

	t.Run("Commit offset backwards (smaller offset)", func(t *testing.T) {
		// First commit higher offset
		err := manager.CommitOffset(ctx, stream, "backwards-group", partition, 100)
		require.NoError(t, err)

		// Commit smaller offset (should be allowed)
		err = manager.CommitOffset(ctx, stream, "backwards-group", partition, 50)
		require.NoError(t, err)

		// Verify smaller offset is stored
		committed, err := manager.GetCommittedOffset(ctx, stream, "backwards-group", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(50), committed)
	})

	t.Run("Commit offset beyond latest offset", func(t *testing.T) {
		// Set latest offset to 50
		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = 50
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		// Commit offset beyond latest (should be allowed)
		err := manager.CommitOffset(ctx, stream, "future-group", partition, 100)
		require.NoError(t, err)

		// Verify it was stored
		committed, err := manager.GetCommittedOffset(ctx, stream, "future-group", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(100), committed)
	})
}

// TestGetCommittedOffset_EdgeCases tests edge cases for GetCommittedOffset
func TestGetCommittedOffset_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	partition := int32(0)

	t.Run("Get offset for non-existent group", func(t *testing.T) {
		offset, err := manager.GetCommittedOffset(ctx, stream, "nonexistent-group", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset) // Should return -1 for no commits
	})

	t.Run("Get offset with context cancellation", func(t *testing.T) {
		// Note: Manager doesn't check context at start, wrapper does
		// This test verifies the method still works even with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// This will work because context is only used for stream validation
		_, err := manager.GetCommittedOffset(ctx, stream, "group", partition)
		assert.NoError(t, err)
	})

	t.Run("Get offset for negative partition", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, "group", -1, 10)
		require.NoError(t, err) // Should allow negative partition

		offset, err := manager.GetCommittedOffset(ctx, stream, "group", -1)
		require.NoError(t, err)
		assert.Equal(t, int64(10), offset)
	})
}

// TestCalculateLag_EdgeCases tests edge cases for lag calculation
func TestCalculateLag_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	t.Run("Lag when committed offset > latest offset", func(t *testing.T) {
		// Commit offset 100
		err := manager.CommitOffset(ctx, stream, group, partition, 100)
		require.NoError(t, err)

		// Set latest offset to 50 (committed is ahead)
		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = 50
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		// Calculate lag
		lag, err := manager.CalculateLag(ctx, stream, group, partition)
		require.NoError(t, err)
		assert.Equal(t, int64(0), lag) // Lag should be 0 (not negative)
	})

	t.Run("Lag when latestOffsetFunc returns error", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, group, partition, 10)
		require.NoError(t, err)

		// Set latestOffsetFunc to return error
		manager.latestOffsetFunc = func(stream string, partition int32) (int64, error) {
			return -1, fmt.Errorf("failed to get latest offset")
		}

		_, err = manager.CalculateLag(ctx, stream, group, partition)
		assert.Error(t, err)
		assert.IsType(t, CalculateLagError{}, err)
	})

	t.Run("Lag when stream has no messages (latest = -1)", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, "no-messages-group", partition, 10)
		require.NoError(t, err)

		// Set latest offset to -1 (no messages)
		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = -1
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		lag, err := manager.CalculateLag(ctx, stream, "no-messages-group", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), lag) // Should return -1 when no messages
	})

	t.Run("Lag with zero offset committed", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, "zero-group", partition, 0)
		require.NoError(t, err)

		// Set latest offset to 100
		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = 100
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		lag, err := manager.CalculateLag(ctx, stream, "zero-group", partition)
		require.NoError(t, err)
		assert.Equal(t, int64(100), lag) // 100 - 0 = 100
	})
}

// TestGetConsumerGroupState_EdgeCases tests edge cases for GetConsumerGroupState
func TestGetConsumerGroupState_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	t.Run("State when no commits and no messages", func(t *testing.T) {
		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = -1
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		state, err := manager.GetConsumerGroupState(ctx, stream, "new-group", partition)
		require.NoError(t, err)

		assert.Equal(t, int64(-1), state.CommittedOffset)
		assert.Equal(t, int64(-1), state.LatestOffset)
		assert.Equal(t, int64(-1), state.Lag)
	})

	t.Run("State with context cancellation", func(t *testing.T) {
		// Note: Manager doesn't check context at start, wrapper does
		// This test verifies the method still works even with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// This will work because context is only used for stream validation
		_, err := manager.GetConsumerGroupState(ctx, stream, group, partition)
		assert.NoError(t, err)
	})
}

// TestListConsumerGroups_EdgeCases tests edge cases for ListConsumerGroups
func TestListConsumerGroups_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	partition := int32(0)

	t.Run("List groups with many consumer groups", func(t *testing.T) {
		// Create many groups
		numGroups := 100
		for i := 0; i < numGroups; i++ {
			group := fmt.Sprintf("group-%d", i)
			err := manager.CommitOffset(ctx, stream, group, partition, int64(i))
			require.NoError(t, err)
		}

		groups, err := manager.ListConsumerGroups(ctx, stream)
		require.NoError(t, err)
		assert.Len(t, groups, numGroups)

		// Verify all groups are present
		for i := 0; i < numGroups; i++ {
			expectedGroup := fmt.Sprintf("group-%d", i)
			assert.Contains(t, groups, expectedGroup)
		}
	})

	t.Run("List groups with context cancellation", func(t *testing.T) {
		// Note: Manager doesn't check context at start, wrapper does
		// This test verifies the method still works even with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// This will work because context is only used for stream validation
		_, err := manager.ListConsumerGroups(ctx, stream)
		assert.NoError(t, err)
	})

	t.Run("List groups with multiple streams", func(t *testing.T) {
		stream2 := createTestStream(t, metaStore, "test-stream-2")

		// Create groups for stream 1
		err := manager.CommitOffset(ctx, stream, "stream1-group", partition, 10)
		require.NoError(t, err)

		// Create groups for stream 2
		err = manager.CommitOffset(ctx, stream2, "stream2-group", partition, 20)
		require.NoError(t, err)

		// List groups for stream 1
		groups1, err := manager.ListConsumerGroups(ctx, stream)
		require.NoError(t, err)
		assert.Contains(t, groups1, "stream1-group")
		assert.NotContains(t, groups1, "stream2-group")

		// List groups for stream 2
		groups2, err := manager.ListConsumerGroups(ctx, stream2)
		require.NoError(t, err)
		assert.Contains(t, groups2, "stream2-group")
		assert.NotContains(t, groups2, "stream1-group")
	})
}

// TestDeleteConsumerGroup_EdgeCases tests edge cases for DeleteConsumerGroup
func TestDeleteConsumerGroup_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	partition := int32(0)

	t.Run("Delete group with multiple partitions", func(t *testing.T) {
		group := "multi-partition-group"

		// Commit offsets for multiple partitions
		err := manager.CommitOffset(ctx, stream, group, 0, 10)
		require.NoError(t, err)
		err = manager.CommitOffset(ctx, stream, group, 1, 20)
		require.NoError(t, err)
		err = manager.CommitOffset(ctx, stream, group, 2, 30)
		require.NoError(t, err)

		// Delete the group (should delete all partitions)
		err = manager.DeleteConsumerGroup(ctx, stream, group)
		require.NoError(t, err)

		// Verify all partitions are deleted
		offset0, err := manager.GetCommittedOffset(ctx, stream, group, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset0)

		offset1, err := manager.GetCommittedOffset(ctx, stream, group, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset1)

		offset2, err := manager.GetCommittedOffset(ctx, stream, group, 2)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset2)
	})

	t.Run("Delete group with context cancellation", func(t *testing.T) {
		// Note: Manager doesn't check context at start, wrapper does
		// This test verifies the method still works even with cancelled context
		group := "cancel-group"
		err := manager.CommitOffset(ctx, stream, group, partition, 10)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// This will work because context is only used for stream validation
		err = manager.DeleteConsumerGroup(ctx, stream, group)
		assert.NoError(t, err)
	})
}

// TestPersistence_EdgeCases tests edge cases for persistence
func TestPersistence_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	t.Run("Load with corrupted JSON", func(t *testing.T) {
		metaStore, err := metastore.NewStore(metadataDir)
		require.NoError(t, err)

		// Create corrupted JSON file
		consumersFile := filepath.Join(metadataDir, DefaultConsumersFile)
		err = os.MkdirAll(filepath.Dir(consumersFile), 0755)
		require.NoError(t, err)

		err = os.WriteFile(consumersFile, []byte("invalid json {"), 0600)
		require.NoError(t, err)

		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		err = manager.Start(ctx)
		assert.Error(t, err) // Should fail to load corrupted JSON
	})

	t.Run("Persistence with empty offsets map", func(t *testing.T) {
		// Use a separate temp dir to avoid conflicts
		tmpDir := t.TempDir()
		metadataDir := filepath.Join(tmpDir, "metadata")

		metaStore, err := metastore.NewStore(metadataDir)
		require.NoError(t, err)

		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		err = manager.Start(ctx)
		require.NoError(t, err)

		// Stop (should flush empty map)
		err = manager.Stop(ctx)
		require.NoError(t, err)

		// Restart and verify
		metaStore2, err := metastore.NewStore(metadataDir)
		require.NoError(t, err)

		manager2 := NewManager(metaStore2, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})
		err = manager2.Start(ctx)
		require.NoError(t, err)

		// Should have no offsets - create a stream first
		stream := createTestStream(t, metaStore2, "test-stream")
		offset, err := manager2.GetCommittedOffset(ctx, stream, "any-group", 0)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset)
	})

	t.Run("Persistence with unicode in group names", func(t *testing.T) {
		// Use a separate temp dir to avoid conflicts
		tmpDir := t.TempDir()
		metadataDir := filepath.Join(tmpDir, "metadata")

		metaStore, err := metastore.NewStore(metadataDir)
		require.NoError(t, err)

		stream := createTestStream(t, metaStore, "test-stream")

		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		err = manager.Start(ctx)
		require.NoError(t, err)

		// Commit offset with unicode group name
		unicodeGroup := "测试组-🧪-グループ"
		err = manager.CommitOffset(ctx, stream, unicodeGroup, 0, 42)
		require.NoError(t, err)

		// Stop and restart
		err = manager.Stop(ctx)
		require.NoError(t, err)

		manager2 := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})
		err = manager2.Start(ctx)
		require.NoError(t, err)

		// Verify unicode group persisted correctly
		offset, err := manager2.GetCommittedOffset(ctx, stream, unicodeGroup, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(42), offset)

		// Verify UTF-8 validity
		assert.True(t, utf8.ValidString(unicodeGroup))
	})
}

// TestConcurrentOperations_EdgeCases tests concurrent operations edge cases
func TestConcurrentOperations_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "concurrent-group"
	partition := int32(0)

	t.Run("Concurrent commits to same group", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 50

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(offset int64) {
				defer wg.Done()
				err := manager.CommitOffset(ctx, stream, group, partition, offset)
				assert.NoError(t, err)
			}(int64(i))
		}

		wg.Wait()

		// Get final offset (should be one of the committed offsets)
		offset, err := manager.GetCommittedOffset(ctx, stream, group, partition)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, offset, int64(0))
		assert.Less(t, offset, int64(numGoroutines))
	})

	t.Run("Concurrent commits and reads", func(t *testing.T) {
		var wg sync.WaitGroup

		// Writers
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(offset int64) {
				defer wg.Done()
				err := manager.CommitOffset(ctx, stream, "read-write-group", partition, offset)
				assert.NoError(t, err)
			}(int64(i * 10))
		}

		// Readers
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := manager.GetCommittedOffset(ctx, stream, "read-write-group", partition)
				assert.NoError(t, err)
			}()
		}

		wg.Wait()
	})

	t.Run("Concurrent lag calculations", func(t *testing.T) {
		err := manager.CommitOffset(ctx, stream, "lag-group", partition, 50)
		require.NoError(t, err)

		offsetMap := make(map[string]int64)
		offsetMap[stream+"|"+string(rune(partition))] = 100
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := manager.CalculateLag(ctx, stream, "lag-group", partition)
				assert.NoError(t, err)
			}()
		}

		wg.Wait()
	})
}

// TestLifecycle_EdgeCases tests lifecycle edge cases
func TestLifecycle_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	t.Run("Operations before Start", func(t *testing.T) {
		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		stream := createTestStream(t, metaStore, "test-stream")

		// Try to commit before start
		err := manager.CommitOffset(ctx, stream, "group", 0, 10)
		assert.NoError(t, err) // Should work (Start will load on first operation)

		// Start should still work
		err = manager.Start(ctx)
		require.NoError(t, err)
	})

	t.Run("Operations after Stop", func(t *testing.T) {
		// Use a separate temp dir to avoid conflicts
		tmpDir := t.TempDir()
		metadataDir := filepath.Join(tmpDir, "metadata")

		metaStore, err := metastore.NewStore(metadataDir)
		require.NoError(t, err)

		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		err = manager.Start(ctx)
		require.NoError(t, err)

		stream := createTestStream(t, metaStore, "test-stream")
		// Commit before stop
		err = manager.CommitOffset(ctx, stream, "group", 0, 10)
		require.NoError(t, err)

		err = manager.Stop(ctx)
		require.NoError(t, err)

		// Try to commit after stop - should still work (manager is not started but can still accept operations)
		// The flush might fail, but the operation should not panic
		err = manager.CommitOffset(ctx, stream, "group", 0, 20)
		// This might fail on flush, but should not panic
		_ = err
	})

	t.Run("Multiple Start calls", func(t *testing.T) {
		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()

		err := manager.Start(ctx)
		require.NoError(t, err)

		// Start again (should be idempotent)
		err = manager.Start(ctx)
		assert.NoError(t, err)

		assert.True(t, manager.Ready())
	})

	t.Run("Multiple Stop calls", func(t *testing.T) {
		manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
			return -1, nil
		})

		ctx := context.Background()
		err := manager.Start(ctx)
		require.NoError(t, err)

		err = manager.Stop(ctx)
		require.NoError(t, err)

		// Stop again (should be idempotent)
		err = manager.Stop(ctx)
		assert.NoError(t, err)
	})
}

// TestLargeScale_EdgeCases tests large scale edge cases
func TestLargeScale_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	t.Run("Very long consumer group name", func(t *testing.T) {
		// Create a very long group name (10KB)
		longGroupName := string(make([]byte, 10000))
		for i := range longGroupName {
			longGroupName = longGroupName[:i] + "a" + longGroupName[i+1:]
		}

		err := manager.CommitOffset(ctx, stream, longGroupName, 0, 100)
		require.NoError(t, err)

		offset, err := manager.GetCommittedOffset(ctx, stream, longGroupName, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(100), offset)
	})

	t.Run("Many offsets for same group different partitions", func(t *testing.T) {
		group := "multi-partition-group"
		numPartitions := 100

		for i := 0; i < numPartitions; i++ {
			err := manager.CommitOffset(ctx, stream, group, int32(i), int64(i*10))
			require.NoError(t, err)
		}

		// Verify all partitions
		for i := 0; i < numPartitions; i++ {
			offset, err := manager.GetCommittedOffset(ctx, stream, group, int32(i))
			require.NoError(t, err)
			assert.Equal(t, int64(i*10), offset)
		}
	})
}

// TestErrorHandling_EdgeCases tests error handling edge cases
func TestErrorHandling_EdgeCases(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetCommittedOffset with invalid stream path format", func(t *testing.T) {
		_, err := manager.GetCommittedOffset(ctx, "", "group", 0)
		// Should validate stream exists
		assert.Error(t, err)
	})

	t.Run("ListConsumerGroups with invalid stream", func(t *testing.T) {
		_, err := manager.ListConsumerGroups(ctx, "invalid/stream/path")
		assert.Error(t, err)
	})

	t.Run("CalculateLag for non-existent group", func(t *testing.T) {
		stream := createTestStream(t, metaStore, "test-stream")

		// Set latest offset to 100
		offsetMap := make(map[string]int64)
		key := stream + "|" + string(rune(0))
		offsetMap[key] = 100
		manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

		lag, err := manager.CalculateLag(ctx, stream, "nonexistent-group", 0)
		require.NoError(t, err)
		// Should return lag = latestOffset + 1 (no commits yet, so lag = latest + 1)
		// latest = 100, so lag = 101
		assert.Equal(t, int64(101), lag)
	})
}

// TestOffsetKey_EdgeCases tests edge cases for offset key generation
func TestOffsetKey_EdgeCases(t *testing.T) {
	t.Run("Offset key with special characters", func(t *testing.T) {
		key1 := offsetKey("stream|with|pipes", "group|with|pipes", 0)
		key2 := offsetKey("stream|with|pipes", "group|with|pipes", 1)

		// Keys should be different for different partitions
		assert.NotEqual(t, key1, key2)

		// Same input should produce same key
		key1Again := offsetKey("stream|with|pipes", "group|with|pipes", 0)
		assert.Equal(t, key1, key1Again)
	})

	t.Run("Offset key with empty strings", func(t *testing.T) {
		key := offsetKey("", "", 0)
		assert.NotEmpty(t, key) // Should still generate a valid key
	})
}

// TestJSONSerialization_EdgeCases tests JSON serialization edge cases
func TestJSONSerialization_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
		return -1, nil
	})

	stream := createTestStream(t, metaStore, "test-stream")

	ctx := context.Background()
	err = manager.Start(ctx)
	require.NoError(t, err)

	// Create offsets with various edge case values
	testCases := []struct {
		name   string
		group  string
		offset int64
		valid  bool
	}{
		{"zero offset", "zero-group", 0, true},
		{"negative one offset", "neg-one-group", -1, true},
		{"max int64", "max-group", 9223372036854775807, true},
		{"unicode group", "测试组", 42, true},
		{"empty group", "", 10, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := manager.CommitOffset(ctx, stream, tc.group, 0, tc.offset)
			if tc.valid {
				require.NoError(t, err)

				// Verify it can be serialized and deserialized
				err = manager.Stop(ctx)
				require.NoError(t, err)

				manager2 := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
					return -1, nil
				})
				err = manager2.Start(ctx)
				require.NoError(t, err)

				offset, err := manager2.GetCommittedOffset(ctx, stream, tc.group, 0)
				require.NoError(t, err)
				assert.Equal(t, tc.offset, offset)

				err = manager2.Stop(ctx)
				require.NoError(t, err)

				// Restart original manager
				err = manager.Start(ctx)
				require.NoError(t, err)
			}
		})
	}

	// Test that JSON can handle the offsets map
	manager.mu.Lock()
	data, err := json.Marshal(manager.offsets)
	manager.mu.Unlock()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it can be unmarshaled
	var unmarshaled map[string]*ConsumerOffset
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, len(manager.offsets), len(unmarshaled))
}
