package metastore

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewStore(tmpDir)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Store should be able to load (even if file doesn't exist)
	// This is expected behavior - file will be created on first write
}

func TestCreateResource(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	require.NoError(t, err)

	config := &ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       ResourceStream,
		Partitions: 1,
		CreatedAt:  time.Now(),
	}

	err = store.CreateResource(config)
	require.NoError(t, err)

	// Verify resource exists
	retrieved, err := store.GetResource(config.GetPath())
	require.NoError(t, err)
	assert.Equal(t, config.Tenant, retrieved.Tenant)
	assert.Equal(t, config.Namespace, retrieved.Namespace)
	assert.Equal(t, config.Name, retrieved.Name)
	assert.Equal(t, config.Type, retrieved.Type)

	// Verify file was created
	metadataFile := filepath.Join(tmpDir, DefaultMetadataFile)
	assert.FileExists(t, metadataFile)
}

func TestGetResource(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	require.NoError(t, err)

	config := &ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       ResourceQueue,
		Partitions: 1,
		CreatedAt:  time.Now(),
	}

	err = store.CreateResource(config)
	require.NoError(t, err)

	// Get existing resource
	retrieved, err := store.GetResource(config.GetPath())
	require.NoError(t, err)
	assert.Equal(t, config.GetPath(), retrieved.GetPath())

	// Get non-existent resource
	_, err = store.GetResource("nonexistent/path")
	assert.Error(t, err)
	assert.IsType(t, ResourceNotFoundError{}, err)
}

func TestListResources(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	require.NoError(t, err)

	// Create multiple resources
	stream1 := &ResourceConfig{
		Tenant: "tenant1", Namespace: "ns1", Name: "stream1", Type: ResourceStream, Partitions: 1, CreatedAt: time.Now(),
	}
	stream2 := &ResourceConfig{
		Tenant: "tenant1", Namespace: "ns1", Name: "stream2", Type: ResourceStream, Partitions: 1, CreatedAt: time.Now(),
	}
	queue1 := &ResourceConfig{
		Tenant: "tenant1", Namespace: "ns1", Name: "queue1", Type: ResourceQueue, Partitions: 1, CreatedAt: time.Now(),
	}

	require.NoError(t, store.CreateResource(stream1))
	require.NoError(t, store.CreateResource(stream2))
	require.NoError(t, store.CreateResource(queue1))

	// List all streams in tenant1/ns1
	resources, err := store.ListResources("tenant1", "ns1", ResourceStream)
	require.NoError(t, err)
	assert.Len(t, resources, 2)

	// List all resources in tenant1/ns1
	resources, err = store.ListResources("tenant1", "ns1", "")
	require.NoError(t, err)
	assert.Len(t, resources, 3)
}

func TestUpdateResource(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	require.NoError(t, err)

	config := &ResourceConfig{
		Tenant: "test-tenant", Namespace: "test-ns", Name: "test-resource",
		Type: ResourceStream, Partitions: 1, CreatedAt: time.Now(),
	}

	require.NoError(t, store.CreateResource(config))

	// Update partitions
	err = store.UpdateResource(config.GetPath(), func(c *ResourceConfig) error {
		c.Partitions = 3
		return nil
	})
	require.NoError(t, err)

	// Verify update
	updated, err := store.GetResource(config.GetPath())
	require.NoError(t, err)
	assert.Equal(t, 3, updated.Partitions)
}

func TestDeleteResource(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	require.NoError(t, err)

	config := &ResourceConfig{
		Tenant: "test-tenant", Namespace: "test-ns", Name: "test-resource",
		Type: ResourceStream, Partitions: 1, CreatedAt: time.Now(),
	}

	require.NoError(t, store.CreateResource(config))

	// Delete resource
	err = store.DeleteResource(config.GetPath())
	require.NoError(t, err)

	// Verify it's gone
	_, err = store.GetResource(config.GetPath())
	assert.Error(t, err)
	assert.IsType(t, ResourceNotFoundError{}, err)
}

func TestLoadAndFlush(t *testing.T) {
	tmpDir := t.TempDir()
	store1, err := NewStore(tmpDir)
	require.NoError(t, err)

	config := &ResourceConfig{
		Tenant: "test-tenant", Namespace: "test-ns", Name: "test-resource",
		Type: ResourceStream, Partitions: 1, CreatedAt: time.Now(),
	}

	require.NoError(t, store1.CreateResource(config))

	// Create a new store and load from disk
	store2, err := NewStore(tmpDir)
	require.NoError(t, err)

	err = store2.Load()
	require.NoError(t, err)

	// Verify resource was loaded
	retrieved, err := store2.GetResource(config.GetPath())
	require.NoError(t, err)
	assert.Equal(t, config.GetPath(), retrieved.GetPath())
}
