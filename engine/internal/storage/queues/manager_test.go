package queues

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestManager_Enqueue(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize queue
	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue a job
	jobID, seq, err := manager.Enqueue(resourcePath, []byte("job payload"), DefaultEnqueueOptions())
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, int64(0), seq)

	// Enqueue another job
	jobID2, seq2, err := manager.Enqueue(resourcePath, []byte("job payload 2"), DefaultEnqueueOptions())
	require.NoError(t, err)
	assert.NotEmpty(t, jobID2)
	assert.NotEqual(t, jobID, jobID2)
	assert.Equal(t, int64(1), seq2)
}

func TestManager_Enqueue_WithDelay(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue with delay
	options := EnqueueOptions{
		Delay:   5 * time.Second,
		Headers: map[string]string{"key": "value"},
	}

	jobID, seq, err := manager.Enqueue(resourcePath, []byte("delayed job"), options)
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, int64(0), seq)

	// Pop ready job - should be nil because of delay
	job, err := manager.PopReadyJob(resourcePath)
	require.NoError(t, err)
	assert.Nil(t, job) // Not yet visible
}

func TestManager_Enqueue_QueueNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	_, _, err := manager.Enqueue("nonexistent/queue", []byte("payload"), DefaultEnqueueOptions())
	assert.Error(t, err)
	assert.IsType(t, QueueNotFoundError{}, err)
}

func TestManager_PopReadyJob(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue jobs
	jobID1, _, err := manager.Enqueue(resourcePath, []byte("job1"), DefaultEnqueueOptions())
	require.NoError(t, err)

	jobID2, _, err := manager.Enqueue(resourcePath, []byte("job2"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Pop ready jobs (should be in order)
	job1, err := manager.PopReadyJob(resourcePath)
	require.NoError(t, err)
	require.NotNil(t, job1)
	assert.Equal(t, jobID1, job1.ID)

	job2, err := manager.PopReadyJob(resourcePath)
	require.NoError(t, err)
	require.NotNil(t, job2)
	assert.Equal(t, jobID2, job2.ID)

	// No more jobs
	job3, err := manager.PopReadyJob(resourcePath)
	require.NoError(t, err)
	assert.Nil(t, job3)
}

func TestManager_AddToInFlight(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue a job
	jobID, _, err := manager.Enqueue(resourcePath, []byte("job payload"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Add to InFlight
	job, err := manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, jobID, job.ID)
	assert.True(t, job.ReserveUntil.After(time.Now()))
	assert.Equal(t, int32(1), job.Attempts)

	// Try to add again - should return existing job
	job2, err := manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, jobID, job2.ID)
}

func TestManager_AddToInFlight_InvalidTimeout(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	jobID, _, err := manager.Enqueue(resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Try with invalid timeout
	_, err = manager.AddToInFlight(resourcePath, jobID, 0)
	assert.Error(t, err)
	assert.IsType(t, InvalidVisibilityTimeoutError{}, err)
}

func TestManager_RemoveFromInFlight(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue and add to InFlight
	jobID, _, err := manager.Enqueue(resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)

	// Remove from InFlight
	err = manager.RemoveFromInFlight(resourcePath, jobID)
	require.NoError(t, err)

	// Try to get - should fail
	_, err = manager.GetInFlight(resourcePath, jobID)
	assert.Error(t, err)
	assert.IsType(t, JobNotFoundError{}, err)
}

func TestManager_GetInFlight(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue and add to InFlight
	jobID, _, err := manager.Enqueue(resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)

	// Get from InFlight
	job, err := manager.GetInFlight(resourcePath, jobID)
	require.NoError(t, err)
	assert.Equal(t, jobID, job.ID)
}

func TestManager_InitializeQueue(t *testing.T) {
	manager, metaStore, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Enqueue some jobs
	_, _, err = manager.Enqueue(resourcePath, []byte("job1"), DefaultEnqueueOptions())
	require.NoError(t, err)

	_, _, err = manager.Enqueue(resourcePath, []byte("job2"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Recreate manager to test recovery
	metaStore2, err := metastore.NewStore(filepath.Join(tmpDir, "metadata"))
	require.NoError(t, err)
	logManager2 := log.NewManager(filepath.Join(tmpDir, "data"))
	manager2 := NewManager(metaStore2, logManager2, filepath.Join(tmpDir, "metadata"))

	// Re-initialize queue (should recover state)
	err = manager2.InitializeQueue(resourcePath)
	require.NoError(t, err)

	// Should be able to pop jobs (recovered from log)
	job, err := manager2.PopReadyJob(resourcePath)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID) // Should have recovered job metadata
}

func TestManager_InitializeQueue_QueueNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.InitializeQueue("nonexistent/queue")
	assert.Error(t, err)
	assert.IsType(t, QueueNotFoundError{}, err)
}
