package consumers

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockLatestOffsetFunc returns a mock function for getting latest offset
func mockLatestOffsetFunc(offsetMap map[string]int64) LatestOffsetFunc {
	return func(stream string, partition int32) (int64, error) {
		key := stream + "|" + string(rune(partition))
		if offset, exists := offsetMap[key]; exists {
			return offset, nil
		}
		return -1, nil // No messages
	}
}

func setupTestManager(t *testing.T) (*Manager, *metastore.Store, string, LatestOffsetFunc, func()) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	// Create mock offset map for testing lag calculation
	offsetMap := make(map[string]int64)
	latestOffsetFunc := mockLatestOffsetFunc(offsetMap)

	manager := NewManager(metaStore, metadataDir, latestOffsetFunc)
	ctx := context.Background()
	err = manager.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		err := manager.Stop(ctx)
		assert.NoError(t, err)
	}

	return manager, metaStore, tmpDir, latestOffsetFunc, cleanup
}

func createTestStream(t *testing.T, metaStore *metastore.Store, name string) string {
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       name,
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	return resourcePath
}

func TestManager_CommitOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Commit an offset
	err := manager.CommitOffset(ctx, stream, group, partition, 42)
	require.NoError(t, err)

	// Verify offset was committed
	offset, err := manager.GetCommittedOffset(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(42), offset)
}

func TestManager_GetCommittedOffset_NoCommits(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Get offset for group with no commits
	offset, err := manager.GetCommittedOffset(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset) // Should return -1 when no commits
}

func TestManager_GetCommittedOffset_StreamNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to get offset for non-existent stream
	_, err := manager.GetCommittedOffset(ctx, "nonexistent/stream", "group", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream not found")
}

func TestManager_CalculateLag(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Set latest offset to 100
	offsetMap := make(map[string]int64)
	offsetMap[stream+"|"+string(rune(partition))] = 100
	manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

	// Commit offset 50
	err := manager.CommitOffset(ctx, stream, group, partition, 50)
	require.NoError(t, err)

	// Calculate lag - should be 50 (100 - 50)
	lag, err := manager.CalculateLag(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(50), lag)
}

func TestManager_CalculateLag_NoCommits(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Set latest offset to 100
	offsetMap := make(map[string]int64)
	offsetMap[stream+"|"+string(rune(partition))] = 100
	manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

	// Calculate lag with no commits - should be 101 (latest + 1)
	lag, err := manager.CalculateLag(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(101), lag)
}

func TestManager_CalculateLag_NoMessages(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Calculate lag with no messages (latest offset = -1)
	lag, err := manager.CalculateLag(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), lag) // Should return -1 when no messages
}

func TestManager_GetConsumerGroupState(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Set latest offset to 100
	offsetMap := make(map[string]int64)
	offsetMap[stream+"|"+string(rune(partition))] = 100
	manager.latestOffsetFunc = mockLatestOffsetFunc(offsetMap)

	// Commit offset 30
	err := manager.CommitOffset(ctx, stream, group, partition, 30)
	require.NoError(t, err)

	// Get consumer group state
	state, err := manager.GetConsumerGroupState(ctx, stream, group, partition)
	require.NoError(t, err)

	assert.Equal(t, stream, state.Stream)
	assert.Equal(t, group, state.Group)
	assert.Equal(t, partition, state.Partition)
	assert.Equal(t, int64(30), state.CommittedOffset)
	assert.Equal(t, int64(100), state.LatestOffset)
	assert.Equal(t, int64(70), state.Lag) // 100 - 30
}

func TestManager_ListConsumerGroups(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	partition := int32(0)

	// Commit offsets for multiple groups
	err := manager.CommitOffset(ctx, stream, "group-1", partition, 10)
	require.NoError(t, err)

	err = manager.CommitOffset(ctx, stream, "group-2", partition, 20)
	require.NoError(t, err)

	err = manager.CommitOffset(ctx, stream, "group-3", partition, 30)
	require.NoError(t, err)

	// List consumer groups
	groups, err := manager.ListConsumerGroups(ctx, stream)
	require.NoError(t, err)

	assert.Len(t, groups, 3)
	assert.Contains(t, groups, "group-1")
	assert.Contains(t, groups, "group-2")
	assert.Contains(t, groups, "group-3")
}

func TestManager_ListConsumerGroups_Empty(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	// List consumer groups for stream with no groups
	groups, err := manager.ListConsumerGroups(ctx, stream)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestManager_DeleteConsumerGroup(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Commit an offset
	err := manager.CommitOffset(ctx, stream, group, partition, 42)
	require.NoError(t, err)

	// Delete the consumer group
	err = manager.DeleteConsumerGroup(ctx, stream, group)
	require.NoError(t, err)

	// Verify offset is gone
	offset, err := manager.GetCommittedOffset(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset) // Should return -1 after deletion
}

func TestManager_DeleteConsumerGroup_NotFound(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	// Try to delete non-existent group
	err := manager.DeleteConsumerGroup(ctx, stream, "nonexistent-group")
	assert.Error(t, err)
	assert.IsType(t, ConsumerGroupNotFoundError{}, err)
}

func TestManager_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore1, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	stream := createTestStream(t, metaStore1, "test-stream")
	group := "test-group"
	partition := int32(0)

	// Create first manager and commit offset
	offsetMap := make(map[string]int64)
	manager1 := NewManager(metaStore1, metadataDir, mockLatestOffsetFunc(offsetMap))
	ctx := context.Background()
	err = manager1.Start(ctx)
	require.NoError(t, err)

	err = manager1.CommitOffset(ctx, stream, group, partition, 100)
	require.NoError(t, err)

	err = manager1.Stop(ctx)
	require.NoError(t, err)

	// Create second manager and verify offset was persisted
	metaStore2, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	manager2 := NewManager(metaStore2, metadataDir, mockLatestOffsetFunc(offsetMap))
	err = manager2.Start(ctx)
	require.NoError(t, err)

	offset, err := manager2.GetCommittedOffset(ctx, stream, group, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offset)

	err = manager2.Stop(ctx)
	require.NoError(t, err)
}

func TestManager_Concurrency(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")
	partition := int32(0)

	// Test concurrent commits
	var wg sync.WaitGroup
	numGoroutines := 10
	numCommitsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(groupID int) {
			defer wg.Done()
			group := "group-" + string(rune(groupID))

			for j := 0; j < numCommitsPerGoroutine; j++ {
				offset := int64(j)
				err := manager.CommitOffset(ctx, stream, group, partition, offset)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all groups exist
	groups, err := manager.ListConsumerGroups(ctx, stream)
	require.NoError(t, err)
	assert.Len(t, groups, numGoroutines)
}

func TestManager_StartStop(t *testing.T) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	manager := NewManager(metaStore, metadataDir, func(stream string, partition int32) (int64, error) {
		return -1, nil
	})

	ctx := context.Background()

	// Test Start
	err = manager.Start(ctx)
	require.NoError(t, err)
	assert.True(t, manager.Ready())

	// Test Stop
	err = manager.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, manager.Ready())
}

func TestManager_CommitOffset_InvalidOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	// Try to commit negative offset (less than -1)
	err := manager.CommitOffset(ctx, stream, "group", 0, -2)
	assert.Error(t, err)
	assert.IsType(t, InvalidOffsetError{}, err)
}

func TestManager_CommitOffset_StreamNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to commit offset for non-existent stream
	err := manager.CommitOffset(ctx, "nonexistent/stream", "group", 0, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream not found")
}
