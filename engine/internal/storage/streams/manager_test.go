package streams

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestManager(t *testing.T) (*Manager, *metastore.Store, *log.Manager, string, func()) {
	tmpDir := t.TempDir()

	metaStore, err := metastore.NewStore(filepath.Join(tmpDir, "metadata"))
	require.NoError(t, err)

	logManager := log.NewManager(filepath.Join(tmpDir, "data"))

	manager := NewManager(metaStore, logManager, filepath.Join(tmpDir, "metadata"))

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return manager, metaStore, logManager, tmpDir, cleanup
}

func TestManager_WriteEvents(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Write events
	events := []Event{
		{Payload: []byte("event1"), Headers: map[string]string{"key": "value1"}},
		{Payload: []byte("event2"), Headers: map[string]string{"key": "value2"}},
		{Payload: []byte("event3"), Headers: nil},
	}

	offsets, err := manager.WriteEvents(resourcePath, events)
	require.NoError(t, err)
	require.Equal(t, 3, len(offsets))

	// Verify offsets are sequential
	assert.Equal(t, int64(0), offsets[0])
	assert.Equal(t, int64(1), offsets[1])
	assert.Equal(t, int64(2), offsets[2])
}

func TestManager_WriteEvents_StreamNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	events := []Event{
		{Payload: []byte("event1")},
	}

	_, err := manager.WriteEvents("nonexistent/stream", events)
	assert.Error(t, err)
	assert.IsType(t, StreamNotFoundError{}, err)
}

func TestManager_ReadFromOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Write events
	events := []Event{
		{Payload: []byte("event1"), Headers: map[string]string{"key": "value1"}},
		{Payload: []byte("event2"), Headers: map[string]string{"key": "value2"}},
		{Payload: []byte("event3"), Headers: nil},
	}

	_, err = manager.WriteEvents(resourcePath, events)
	require.NoError(t, err)

	// Read from offset 0
	messages, err := manager.ReadFromOffset(resourcePath, 0, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(messages))

	assert.Equal(t, int64(0), messages[0].Offset)
	assert.Equal(t, []byte("event1"), messages[0].Payload)
	assert.Equal(t, "value1", messages[0].Headers["key"])

	assert.Equal(t, int64(1), messages[1].Offset)
	assert.Equal(t, []byte("event2"), messages[1].Payload)

	assert.Equal(t, int64(2), messages[2].Offset)
	assert.Equal(t, []byte("event3"), messages[2].Payload)
}

func TestManager_ReadFromOffset_Limited(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Write 5 events
	events := make([]Event, 5)
	for i := 0; i < 5; i++ {
		events[i] = Event{Payload: []byte{byte('0' + i)}}
	}

	_, err = manager.WriteEvents(resourcePath, events)
	require.NoError(t, err)

	// Read only 2 messages
	messages, err := manager.ReadFromOffset(resourcePath, 0, 0, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(messages))

	assert.Equal(t, int64(0), messages[0].Offset)
	assert.Equal(t, int64(1), messages[1].Offset)
}

func TestManager_GetLatestOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// No messages yet
	offset, err := manager.GetLatestOffset(resourcePath, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset)

	// Write events
	events := []Event{
		{Payload: []byte("event1")},
		{Payload: []byte("event2")},
		{Payload: []byte("event3")},
	}

	_, err = manager.WriteEvents(resourcePath, events)
	require.NoError(t, err)

	// Latest offset should be 2 (last written)
	offset, err = manager.GetLatestOffset(resourcePath, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(2), offset)
}

func TestManager_InitializeStream(t *testing.T) {
	manager, metaStore, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Write some events
	events := []Event{
		{Payload: []byte("event1")},
		{Payload: []byte("event2")},
	}

	_, err = manager.WriteEvents(resourcePath, events)
	require.NoError(t, err)

	// Recreate manager to test recovery
	metaStore2, err := metastore.NewStore(filepath.Join(tmpDir, "metadata"))
	require.NoError(t, err)
	logManager2 := log.NewManager(filepath.Join(tmpDir, "data"))
	manager2 := NewManager(metaStore2, logManager2, filepath.Join(tmpDir, "metadata"))

	// Re-initialize stream (should recover offsets)
	err = manager2.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Latest offset should be recovered
	offset, err := manager2.GetLatestOffset(resourcePath, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), offset)
}

func TestManager_ReadFromOffset_InvalidOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeStream(resourcePath)
	require.NoError(t, err)

	// Try to read from negative offset
	_, err = manager.ReadFromOffset(resourcePath, 0, -1, 10)
	assert.Error(t, err)
	assert.IsType(t, InvalidOffsetError{}, err)
}
