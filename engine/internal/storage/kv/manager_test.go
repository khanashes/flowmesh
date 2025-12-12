package kv

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestManager(t *testing.T) (*Manager, *metastore.Store, string, func()) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")
	kvDir := filepath.Join(tmpDir, "kv")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	manager := NewManager(metaStore, kvDir)
	ctx := context.Background()
	err = manager.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		err := manager.Stop(ctx)
		assert.NoError(t, err)
	}

	return manager, metaStore, tmpDir, cleanup
}

func TestManager_SetGet(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Set a value
	key := "test-key"
	value := []byte("test-value")
	err = manager.Set(ctx, resourcePath, key, value, DefaultSetOptions())
	require.NoError(t, err)

	// Get the value
	retrieved, err := manager.Get(ctx, resourcePath, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestManager_SetGet_WithTTL(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv-ttl",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Set a value with TTL
	key := "test-key-ttl"
	value := []byte("test-value-ttl")
	options := SetOptions{TTL: 100 * time.Millisecond}
	err = manager.Set(ctx, resourcePath, key, value, options)
	require.NoError(t, err)

	// Get immediately - should work
	retrieved, err := manager.Get(ctx, resourcePath, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Try to get again - should be expired
	retrieved, err = manager.Get(ctx, resourcePath, key)
	require.Error(t, err)
	assert.IsType(t, KeyExpiredError{}, err)
	assert.Nil(t, retrieved)
}

func TestManager_Delete(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv-delete",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Set a value
	key := "test-key"
	value := []byte("test-value")
	err = manager.Set(ctx, resourcePath, key, value, DefaultSetOptions())
	require.NoError(t, err)

	// Delete the key
	err = manager.Delete(ctx, resourcePath, key)
	require.NoError(t, err)

	// Try to get - should not exist
	retrieved, err := manager.Get(ctx, resourcePath, key)
	require.Error(t, err)
	assert.IsType(t, KeyNotFoundError{}, err)
	assert.Nil(t, retrieved)
}

func TestManager_Exists(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv-exists",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Key doesn't exist
	exists, err := manager.Exists(ctx, resourcePath, "nonexistent")
	require.NoError(t, err)
	assert.False(t, exists)

	// Set a value
	key := "test-key"
	value := []byte("test-value")
	err = manager.Set(ctx, resourcePath, key, value, DefaultSetOptions())
	require.NoError(t, err)

	// Key exists
	exists, err = manager.Exists(ctx, resourcePath, key)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestManager_ListKeys(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv-list",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Set multiple keys
	keys := []string{"key1", "key2", "prefix:key1", "prefix:key2"}
	for _, key := range keys {
		err = manager.Set(ctx, resourcePath, key, []byte("value"), DefaultSetOptions())
		require.NoError(t, err)
	}

	// List all keys
	allKeys, err := manager.ListKeys(ctx, resourcePath, "")
	require.NoError(t, err)
	assert.Len(t, allKeys, 4)

	// List keys with prefix
	prefixedKeys, err := manager.ListKeys(ctx, resourcePath, "prefix:")
	require.NoError(t, err)
	assert.Len(t, prefixedKeys, 2)
	assert.Contains(t, prefixedKeys, "prefix:key1")
	assert.Contains(t, prefixedKeys, "prefix:key2")
}

func TestManager_KVStoreNotFound(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to use non-existent KV store
	_, err := manager.Get(ctx, "nonexistent/kv/store", "key")
	require.Error(t, err)
	assert.IsType(t, KVStoreNotFoundError{}, err)
}

func TestManager_KeyNotFound(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Try to get non-existent key
	_, err = manager.Get(ctx, resourcePath, "nonexistent")
	require.Error(t, err)
	assert.IsType(t, KeyNotFoundError{}, err)
}

func TestManager_InvalidKey(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "test-kv",
		Type:      metastore.ResourceKV,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	// Try to set with empty key
	err = manager.Set(ctx, resourcePath, "", []byte("value"), DefaultSetOptions())
	require.Error(t, err)
	assert.IsType(t, InvalidKeyError{}, err)
}

func TestManager_StartStop(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Already started in setup
	assert.True(t, manager.Ready())

	// Stop
	err := manager.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, manager.Ready())

	// Start again
	err = manager.Start(ctx)
	require.NoError(t, err)
	assert.True(t, manager.Ready())
}
