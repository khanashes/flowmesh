package storage

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := New(tmpDir)
	require.NoError(t, err)
	assert.NotNil(t, storage)
	assert.NotNil(t, storage.MetaStore())
	assert.NotNil(t, storage.LogManager())
	assert.NotNil(t, storage.StreamManager())
	assert.NotNil(t, storage.QueueManager())
	assert.NotNil(t, storage.KVManager())
	assert.NotNil(t, storage.Paths())
}

func TestStorage_Close(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := New(tmpDir)
	require.NoError(t, err)

	// Create a resource to have some state
	config := &metastore.ResourceConfig{
		Tenant:     "test",
		Namespace:  "test",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
		CreatedAt:  time.Now(),
	}

	err = storage.MetaStore().CreateResource(config)
	require.NoError(t, err)

	// Close should succeed
	ctx := context.Background()
	err = storage.Close(ctx)
	assert.NoError(t, err)

	// Closing again should be safe
	err = storage.Close(ctx)
	assert.NoError(t, err)
}

func TestStorage_Validate(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := New(tmpDir)
	require.NoError(t, err)

	// Should validate successfully
	ctx := context.Background()
	err = storage.Validate(ctx)
	assert.NoError(t, err)
}

func TestStorage_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create storage
	storage, err := New(tmpDir)
	require.NoError(t, err)

	// Create a stream resource
	streamConfig := &metastore.ResourceConfig{
		Tenant:     "tenant1",
		Namespace:  "ns1",
		Name:       "my-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
		CreatedAt:  time.Now(),
	}

	err = storage.MetaStore().CreateResource(streamConfig)
	require.NoError(t, err)

	// Verify it was created
	retrieved, err := storage.MetaStore().GetResource(streamConfig.GetPath())
	require.NoError(t, err)
	assert.Equal(t, streamConfig.GetPath(), retrieved.GetPath())

	// Open a segment for this resource
	writer, err := storage.LogManager().OpenSegment(streamConfig.GetPath(), 0)
	require.NoError(t, err)
	assert.NotNil(t, writer)

	// Close gracefully
	ctx := context.Background()
	err = storage.Close(ctx)
	assert.NoError(t, err)
}

func TestStorage_StartStop_WithKVManager(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := New(tmpDir)
	require.NoError(t, err)

	ctx := context.Background()

	// Start storage (should start KV manager)
	err = storage.Start(ctx)
	require.NoError(t, err)
	assert.True(t, storage.Ready())
	assert.True(t, storage.KVManager().Ready())

	// Stop storage (should stop KV manager)
	err = storage.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, storage.KVManager().Ready())
}
