package queues

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_Reserve(t *testing.T) {
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
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job payload"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Reserve the job
	ctx := context.Background()
	options := DefaultReserveOptions()
	options.VisibilityTimeout = 30 * time.Second

	job, err := manager.Reserve(ctx, resourcePath, options)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, jobID, job.ID)
	assert.True(t, job.ReserveUntil.After(time.Now()))
}

func TestManager_Reserve_LongPolling(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to reserve with long polling (no jobs available)
	options := DefaultReserveOptions()
	options.VisibilityTimeout = 30 * time.Second
	options.LongPollTimeout = 1 * time.Second
	options.MaxWaitTime = 1 * time.Second

	job, err := manager.Reserve(ctx, resourcePath, options)
	require.NoError(t, err)
	assert.Nil(t, job) // Should timeout with no job
}

func TestManager_ACK(t *testing.T) {
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

	// Enqueue and reserve a job
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)

	// ACK the job
	ctx := context.Background()
	err = manager.ACK(ctx, resourcePath, jobID)
	require.NoError(t, err)

	// ACK again (should be idempotent)
	err = manager.ACK(ctx, resourcePath, jobID)
	require.NoError(t, err)

	// Job should not be in InFlight
	_, err = manager.GetInFlight(resourcePath, jobID)
	assert.Error(t, err)
	assert.IsType(t, JobNotFoundError{}, err)
}

func TestManager_NACK(t *testing.T) {
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

	// Set retry policy
	policy := DefaultRetryPolicy()
	manager.mu.Lock()
	manager.retryPolicies[resourcePath] = &policy
	manager.mu.Unlock()

	// Enqueue and reserve a job
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)

	// NACK the job
	ctx := context.Background()
	err = manager.NACK(ctx, resourcePath, jobID)
	require.NoError(t, err)

	// Job should be back in ReadyHeap with updated attempts
	// Note: Job may have a backoff delay, so we check metadata instead of popping
	job, err := manager.GetJobMetadata(ctx, resourcePath, jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, jobID, job.ID)
	assert.Equal(t, int32(2), job.Attempts)                                            // Original attempt + NACK increment
	assert.True(t, job.VisibleAt.After(time.Now()) || job.VisibleAt.Equal(time.Now())) // May have backoff delay
}

func TestManager_NACK_MaxAttempts(t *testing.T) {
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

	// Set retry policy with max attempts = 2
	policy := DefaultRetryPolicy()
	policy.MaxAttempts = 2
	manager.mu.Lock()
	manager.retryPolicies[resourcePath] = &policy
	manager.mu.Unlock()

	// Enqueue and reserve a job
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Manually set attempts to max
	manager.mu.Lock()
	state := manager.queueStates[resourcePath]
	state.mu.Lock()
	job := state.InFlight[jobID]
	if job == nil {
		// Add to InFlight first
		job = &JobMetadata{ID: jobID, Attempts: 2}
		state.InFlight[jobID] = job
	} else {
		job.Attempts = 2
	}
	state.mu.Unlock()
	manager.mu.Unlock()

	// NACK should fail with MaxAttemptsExceededError
	ctx := context.Background()
	err = manager.NACK(ctx, resourcePath, jobID)
	assert.Error(t, err)
	assert.IsType(t, MaxAttemptsExceededError{}, err)
}

func TestManager_Receive(t *testing.T) {
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

	// Enqueue multiple jobs
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job1"), DefaultEnqueueOptions())
	require.NoError(t, err)
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job2"), DefaultEnqueueOptions())
	require.NoError(t, err)
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job3"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Receive jobs
	ctx := context.Background()
	options := DefaultReserveOptions()
	options.VisibilityTimeout = 30 * time.Second

	jobs, err := manager.Receive(ctx, resourcePath, 2, options)
	require.NoError(t, err)
	assert.Len(t, jobs, 2)
}

func TestManager_RegisterWorker(t *testing.T) {
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

	ctx := context.Background()

	// Register a worker
	err = manager.RegisterWorker(ctx, resourcePath, "worker1")
	require.NoError(t, err)

	// Heartbeat
	err = manager.Heartbeat(ctx, resourcePath, "worker1")
	require.NoError(t, err)

	// Unregister
	err = manager.UnregisterWorker(ctx, resourcePath, "worker1")
	require.NoError(t, err)
}

func TestManager_GetJobPayload(t *testing.T) {
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
	payload := []byte("test payload")
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, payload, DefaultEnqueueOptions())
	require.NoError(t, err)

	// Get job payload
	ctx := context.Background()
	retrievedPayload, err := manager.GetJobPayload(ctx, resourcePath, jobID)
	require.NoError(t, err)
	assert.Equal(t, payload, retrievedPayload)
}

func TestManager_GetQueueStats(t *testing.T) {
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

	// Enqueue some jobs
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job1"), DefaultEnqueueOptions())
	require.NoError(t, err)
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job2"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Reserve one job
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job3"), DefaultEnqueueOptions())
	require.NoError(t, err)
	_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
	require.NoError(t, err)

	// Get stats
	ctx := context.Background()
	stats, err := manager.GetQueueStats(ctx, resourcePath)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.TotalJobs)
	assert.Equal(t, int64(2), stats.PendingJobs)
	assert.Equal(t, int64(1), stats.InFlightJobs)
}
