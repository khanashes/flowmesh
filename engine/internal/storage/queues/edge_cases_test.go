package queues

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReserve_EdgeCases tests edge cases for Reserve operation
func TestReserve_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("Reserve with no jobs available", func(t *testing.T) {
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

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second
		options.LongPollTimeout = 0 // No long polling

		job, err := manager.Reserve(ctx, resourcePath, options)
		require.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("Reserve with context cancellation", func(t *testing.T) {
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

		// Enqueue a job first so Reserve has something to work with
		_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second

		job, err := manager.Reserve(ctx, resourcePath, options)
		// Reserve checks context in PopReadyJobWithContext, which should return context.Canceled
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
		assert.Nil(t, job)
	})

	t.Run("Reserve with zero visibility timeout", func(t *testing.T) {
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

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 0

		_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		job, err := manager.Reserve(ctx, resourcePath, options)
		assert.Error(t, err)
		assert.IsType(t, InvalidVisibilityTimeoutError{}, err)
		assert.Nil(t, job)
	})

	t.Run("Reserve on non-existent queue", func(t *testing.T) {
		manager, _, _, _, cleanup := setupTestManager(t)
		defer cleanup()

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second

		job, err := manager.Reserve(ctx, "nonexistent/queue", options)
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
		assert.Nil(t, job)
	})

	t.Run("Reserve with very short visibility timeout", func(t *testing.T) {
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

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 1 * time.Millisecond // Very short (use millisecond instead of nanosecond)

		job, err := manager.Reserve(ctx, resourcePath, options)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, jobID, job.ID)
		assert.True(t, job.ReserveUntil.After(time.Now()))
	})

	t.Run("Reserve with very long visibility timeout", func(t *testing.T) {
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

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 24 * time.Hour // Very long

		nowBeforeReserve := time.Now()
		job, err := manager.Reserve(ctx, resourcePath, options)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, jobID, job.ID)
		// ReserveUntil should be approximately 24 hours from when Reserve was called
		// Allow some margin for test execution time
		expectedMin := nowBeforeReserve.Add(23*time.Hour + 59*time.Minute)
		expectedMax := nowBeforeReserve.Add(24*time.Hour + 2*time.Minute)
		assert.True(t, job.ReserveUntil.After(expectedMin) || job.ReserveUntil.Equal(expectedMin),
			"ReserveUntil should be at least 23h59m in the future, got %v", job.ReserveUntil.Sub(nowBeforeReserve))
		assert.True(t, job.ReserveUntil.Before(expectedMax),
			"ReserveUntil should be less than 24h2m in the future, got %v", job.ReserveUntil.Sub(nowBeforeReserve))
	})
}

// TestACK_EdgeCases tests edge cases for ACK operation
func TestACK_EdgeCases(t *testing.T) {
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

	ctx := context.Background()

	t.Run("ACK non-existent job", func(t *testing.T) {
		err := manager.ACK(ctx, resourcePath, "nonexistent-job-id")
		// Should be idempotent - return nil
		assert.NoError(t, err)
	})

	t.Run("ACK job that's not in InFlight", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// ACK without reserving (job is still in ReadyHeap)
		err = manager.ACK(ctx, resourcePath, jobID)
		// Should be idempotent - return nil
		assert.NoError(t, err)
	})

	t.Run("ACK same job multiple times (idempotency)", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		// ACK first time
		err = manager.ACK(ctx, resourcePath, jobID)
		require.NoError(t, err)

		// ACK second time (should be idempotent)
		err = manager.ACK(ctx, resourcePath, jobID)
		assert.NoError(t, err)

		// ACK third time (should be idempotent)
		err = manager.ACK(ctx, resourcePath, jobID)
		assert.NoError(t, err)
	})

	t.Run("ACK with context cancellation", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = manager.ACK(ctx, resourcePath, jobID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("ACK on non-existent queue", func(t *testing.T) {
		err := manager.ACK(ctx, "nonexistent/queue", "job-id")
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
	})
}

// TestNACK_EdgeCases tests edge cases for NACK operation
func TestNACK_EdgeCases(t *testing.T) {
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

	ctx := context.Background()

	t.Run("NACK non-existent job", func(t *testing.T) {
		err := manager.NACK(ctx, resourcePath, "nonexistent-job-id")
		assert.Error(t, err)
		assert.IsType(t, JobNotFoundError{}, err)
	})

	t.Run("NACK job that's not in InFlight", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// NACK without reserving (job is still in ReadyHeap)
		err = manager.NACK(ctx, resourcePath, jobID)
		assert.Error(t, err)
		assert.IsType(t, JobNotFoundError{}, err)
	})

	t.Run("NACK with max attempts exceeded", func(t *testing.T) {
		// Set retry policy with max attempts = 1
		policy := DefaultRetryPolicy()
		policy.MaxAttempts = 1
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Manually set attempts to max
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		job := &JobMetadata{ID: jobID, Attempts: 1}
		state.InFlight[jobID] = job
		state.mu.Unlock()
		manager.mu.Unlock()

		err = manager.NACK(ctx, resourcePath, jobID)
		assert.Error(t, err)
		assert.IsType(t, MaxAttemptsExceededError{}, err)
	})

	t.Run("NACK with different backoff strategies", func(t *testing.T) {
		// Test Fixed backoff
		policy := DefaultRetryPolicy()
		policy.BackoffStrategy = BackoffStrategyFixed
		policy.InitialBackoff = 5 * time.Second
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		err = manager.NACK(ctx, resourcePath, jobID)
		require.NoError(t, err)

		job, err := manager.GetJobMetadata(ctx, resourcePath, jobID)
		require.NoError(t, err)
		assert.True(t, job.VisibleAt.After(time.Now()))
		assert.True(t, job.VisibleAt.Before(time.Now().Add(6*time.Second)))
	})

	t.Run("NACK with context cancellation", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = manager.NACK(ctx, resourcePath, jobID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("NACK on non-existent queue", func(t *testing.T) {
		err := manager.NACK(ctx, "nonexistent/queue", "job-id")
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
	})
}

// TestReceive_EdgeCases tests edge cases for Receive operation
func TestReceive_EdgeCases(t *testing.T) {
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

	ctx := context.Background()
	options := DefaultReserveOptions()
	options.VisibilityTimeout = 30 * time.Second

	t.Run("Receive with maxJobs = 0", func(t *testing.T) {
		_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job1"), DefaultEnqueueOptions())
		require.NoError(t, err)

		jobs, err := manager.Receive(ctx, resourcePath, 0, options)
		require.NoError(t, err)
		assert.Len(t, jobs, 1) // Should default to 1
	})

	t.Run("Receive with maxJobs > 100", func(t *testing.T) {
		// Enqueue 5 jobs
		for i := 0; i < 5; i++ {
			_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
			require.NoError(t, err)
		}

		jobs, err := manager.Receive(ctx, resourcePath, 200, options)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(jobs), 100) // Should be capped at 100
	})

	t.Run("Receive with no jobs available", func(t *testing.T) {
		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second
		options.LongPollTimeout = 0 // No long polling

		jobs, err := manager.Receive(ctx, resourcePath, 10, options)
		require.NoError(t, err)
		assert.Len(t, jobs, 0)
	})

	t.Run("Receive with context cancellation", func(t *testing.T) {
		// Enqueue a job first so Receive has something to work with
		_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second

		jobs, err := manager.Receive(ctx, resourcePath, 10, options)
		// Receive checks context at the beginning via ReceiveWithContext
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
		assert.Nil(t, jobs)
	})

	t.Run("Receive on non-existent queue", func(t *testing.T) {
		jobs, err := manager.Receive(ctx, "nonexistent/queue", 10, options)
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
		assert.Nil(t, jobs)
	})
}

// TestWorkerRegistration_EdgeCases tests edge cases for worker registration
func TestWorkerRegistration_EdgeCases(t *testing.T) {
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

	t.Run("Register worker on non-existent queue", func(t *testing.T) {
		err := manager.RegisterWorker(ctx, "nonexistent/queue", "worker1")
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
	})

	t.Run("Unregister non-existent worker", func(t *testing.T) {
		err := manager.UnregisterWorker(ctx, resourcePath, "nonexistent-worker")
		assert.Error(t, err)
		assert.IsType(t, WorkerNotFoundError{}, err)
	})

	t.Run("Heartbeat for non-existent worker", func(t *testing.T) {
		err := manager.Heartbeat(ctx, resourcePath, "nonexistent-worker")
		assert.Error(t, err)
		assert.IsType(t, WorkerNotFoundError{}, err)
	})

	t.Run("Multiple workers on same queue", func(t *testing.T) {
		err := manager.RegisterWorker(ctx, resourcePath, "worker1")
		require.NoError(t, err)

		err = manager.RegisterWorker(ctx, resourcePath, "worker2")
		require.NoError(t, err)

		err = manager.RegisterWorker(ctx, resourcePath, "worker3")
		require.NoError(t, err)

		// All should be registered
		err = manager.Heartbeat(ctx, resourcePath, "worker1")
		assert.NoError(t, err)

		err = manager.Heartbeat(ctx, resourcePath, "worker2")
		assert.NoError(t, err)

		err = manager.Heartbeat(ctx, resourcePath, "worker3")
		assert.NoError(t, err)
	})

	t.Run("Unregister worker with active jobs", func(t *testing.T) {
		err := manager.InitializeQueue(resourcePath)
		require.NoError(t, err)

		// Register worker
		err = manager.RegisterWorker(ctx, resourcePath, "worker1")
		require.NoError(t, err)

		// Enqueue and reserve a job
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		// Manually add job to worker's active jobs
		manager.mu.Lock()
		worker := manager.workers[resourcePath]["worker1"]
		worker.ActiveJobs = append(worker.ActiveJobs, jobID)
		manager.mu.Unlock()

		// Unregister worker (should requeue the job)
		err = manager.UnregisterWorker(ctx, resourcePath, "worker1")
		require.NoError(t, err)

		// Job should be back in ReadyHeap
		job, err := manager.PopReadyJob(resourcePath)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, jobID, job.ID)
	})

	t.Run("Register same worker twice", func(t *testing.T) {
		err := manager.RegisterWorker(ctx, resourcePath, "worker1")
		require.NoError(t, err)

		// Register again (should update heartbeat)
		err = manager.RegisterWorker(ctx, resourcePath, "worker1")
		assert.NoError(t, err)
	})
}

// TestGetJobPayload_EdgeCases tests edge cases for GetJobPayload
func TestGetJobPayload_EdgeCases(t *testing.T) {
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

	ctx := context.Background()

	t.Run("Get payload for non-existent job", func(t *testing.T) {
		payload, err := manager.GetJobPayload(ctx, resourcePath, "nonexistent-job-id")
		assert.Error(t, err)
		assert.IsType(t, JobNotFoundError{}, err)
		assert.Nil(t, payload)
	})

	t.Run("Get payload with context cancellation", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		payload, err := manager.GetJobPayload(ctx, resourcePath, jobID)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
		assert.Nil(t, payload)
	})

	t.Run("Get payload on non-existent queue", func(t *testing.T) {
		payload, err := manager.GetJobPayload(ctx, "nonexistent/queue", "job-id")
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
		assert.Nil(t, payload)
	})
}

// TestGetQueueStats_EdgeCases tests edge cases for GetQueueStats
func TestGetQueueStats_EdgeCases(t *testing.T) {
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

	ctx := context.Background()

	t.Run("Stats for empty queue", func(t *testing.T) {
		stats, err := manager.GetQueueStats(ctx, resourcePath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), stats.TotalJobs)
		assert.Equal(t, int64(0), stats.PendingJobs)
		assert.Equal(t, int64(0), stats.InFlightJobs)
		assert.Equal(t, time.Duration(0), stats.OldestJobAge)
	})

	t.Run("Stats for queue with only in-flight jobs", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		_, err = manager.AddToInFlight(resourcePath, jobID, 30*time.Second)
		require.NoError(t, err)

		stats, err := manager.GetQueueStats(ctx, resourcePath)
		require.NoError(t, err)
		assert.Equal(t, int64(1), stats.TotalJobs)
		assert.Equal(t, int64(0), stats.PendingJobs)
		assert.Equal(t, int64(1), stats.InFlightJobs)
	})

	t.Run("Stats for queue with only pending jobs", func(t *testing.T) {
		_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job1"), DefaultEnqueueOptions())
		require.NoError(t, err)
		_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("job2"), DefaultEnqueueOptions())
		require.NoError(t, err)

		stats, err := manager.GetQueueStats(ctx, resourcePath)
		require.NoError(t, err)
		assert.Equal(t, int64(3), stats.TotalJobs) // 1 from previous test + 2 new
		assert.GreaterOrEqual(t, stats.PendingJobs, int64(2))
		assert.GreaterOrEqual(t, stats.OldestJobAge, time.Duration(0))
	})

	t.Run("Stats on non-existent queue", func(t *testing.T) {
		stats, err := manager.GetQueueStats(ctx, "nonexistent/queue")
		assert.Error(t, err)
		assert.IsType(t, QueueNotFoundError{}, err)
		assert.Nil(t, stats)
	})

	t.Run("Stats with context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		stats, err := manager.GetQueueStats(ctx, resourcePath)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
		assert.Nil(t, stats)
	})
}

// TestScheduler_EdgeCases tests edge cases for scheduler
func TestScheduler_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	metaStore, err := metastore.NewStore(filepath.Join(tmpDir, "metadata"))
	require.NoError(t, err)

	logManager := log.NewManager(filepath.Join(tmpDir, "data"))

	manager := NewManager(metaStore, logManager, filepath.Join(tmpDir, "metadata"))

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
	err = metaStore.CreateResource(config)
	require.NoError(t, err)

	err = manager.InitializeQueue(resourcePath)
	require.NoError(t, err)

	t.Run("Process expired jobs with no retry policy", func(t *testing.T) {
		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Manually add to InFlight with expired timeout
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		allJobs := state.ReadyHeap.GetAll()
		var job *JobMetadata
		for _, j := range allJobs {
			if j.ID == jobID {
				job = j
				break
			}
		}
		if job != nil {
			state.ReadyHeap.Remove(jobID)
			job.ReserveUntil = time.Now().Add(-1 * time.Second) // Expired
			state.InFlight[jobID] = job
		}
		state.mu.Unlock()
		manager.mu.Unlock()

		// Process expired jobs (should use default policy)
		manager.scheduler.processExpiredJobs(resourcePath, state)

		// Job should be requeued
		ctx := context.Background()
		jobMeta, err := manager.GetJobMetadata(ctx, resourcePath, jobID)
		require.NoError(t, err)
		require.NotNil(t, jobMeta)
	})

	t.Run("Process expired jobs with zero backoff", func(t *testing.T) {
		policy := DefaultRetryPolicy()
		policy.InitialBackoff = 0
		policy.MaxBackoff = 0
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Manually add to InFlight with expired timeout
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		allJobs := state.ReadyHeap.GetAll()
		var job *JobMetadata
		for _, j := range allJobs {
			if j.ID == jobID {
				job = j
				break
			}
		}
		if job != nil {
			state.ReadyHeap.Remove(jobID)
			job.ReserveUntil = time.Now().Add(-1 * time.Second) // Expired
			state.InFlight[jobID] = job
		}
		state.mu.Unlock()
		manager.mu.Unlock()

		// Process expired jobs
		manager.scheduler.processExpiredJobs(resourcePath, state)

		// Job should be immediately visible
		poppedJob, err := manager.PopReadyJob(resourcePath)
		require.NoError(t, err)
		require.NotNil(t, poppedJob)
		assert.Equal(t, jobID, poppedJob.ID)
	})

	t.Run("Scheduler with no queue manager set", func(t *testing.T) {
		scheduler := NewScheduler(metaStore, logManager)
		// Don't set queue manager

		// Should not panic
		scheduler.processAllQueues()
	})
}

// TestRetryPolicy_EdgeCases tests edge cases for retry policy
func TestRetryPolicy_EdgeCases(t *testing.T) {
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

	ctx := context.Background()

	t.Run("Exponential backoff with many attempts", func(t *testing.T) {
		policy := DefaultRetryPolicy()
		policy.BackoffStrategy = BackoffStrategyExponential
		policy.InitialBackoff = 1 * time.Second
		policy.MaxBackoff = 10 * time.Second
		policy.BackoffMultiplier = 2.0
		policy.MaxAttempts = 15 // Allow high attempts
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Get the actual job from ReadyHeap and move it to InFlight with high attempts
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		// Find the job in ReadyHeap
		allJobs := state.ReadyHeap.GetAll()
		var job *JobMetadata
		for _, j := range allJobs {
			if j.ID == jobID {
				job = j
				break
			}
		}
		require.NotNil(t, job, "Job should be in ReadyHeap")
		// Remove from ReadyHeap
		state.ReadyHeap.Remove(jobID)
		// Set high attempts
		job.Attempts = 10
		// Add to InFlight
		state.InFlight[jobID] = job
		state.mu.Unlock()
		manager.mu.Unlock()

		// Capture time before NACK
		nowBeforeNACK := time.Now()
		err = manager.NACK(ctx, resourcePath, jobID)
		require.NoError(t, err)

		// Get the job back (should be in ReadyHeap now)
		var retrievedJob *JobMetadata
		retrievedJob, err = manager.GetJobMetadata(ctx, resourcePath, jobID)
		require.NoError(t, err)
		require.NotNil(t, retrievedJob)

		// Backoff should be capped at MaxBackoff (10 seconds)
		// For exponential: attempts=10, multiplier=2, initial=1s -> 1 * 2^9 = 512s, but capped at 10s
		// Verify attempts were incremented
		assert.Equal(t, int32(11), retrievedJob.Attempts, "Attempts should be incremented from 10 to 11")

		// Verify VisibleAt is set correctly (should be around 10 seconds from when NACK was called)
		assert.True(t, retrievedJob.VisibleAt.After(nowBeforeNACK), "VisibleAt should be after NACK time")
		delay := retrievedJob.VisibleAt.Sub(nowBeforeNACK)
		// Allow reasonable margin: should be around 10 seconds (capped), allow 8-12 seconds
		assert.True(t, delay >= 8*time.Second && delay <= 12*time.Second,
			"Backoff delay should be around 10 seconds (8-12s range), got %v", delay)
	})

	t.Run("Linear backoff calculation", func(t *testing.T) {
		policy := DefaultRetryPolicy()
		policy.BackoffStrategy = BackoffStrategyLinear
		policy.InitialBackoff = 2 * time.Second
		policy.MaxAttempts = 10 // Allow high attempts
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Get the actual job from ReadyHeap and move it to InFlight with attempts=3
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		// Find the job in ReadyHeap
		allJobs := state.ReadyHeap.GetAll()
		var job *JobMetadata
		for _, j := range allJobs {
			if j.ID == jobID {
				job = j
				break
			}
		}
		require.NotNil(t, job, "Job should be in ReadyHeap")
		// Remove from ReadyHeap
		state.ReadyHeap.Remove(jobID)
		// Set attempts to 3
		job.Attempts = 3
		// Add to InFlight
		state.InFlight[jobID] = job
		state.mu.Unlock()
		manager.mu.Unlock()

		// Capture time before NACK
		nowBeforeNACK := time.Now()
		err = manager.NACK(ctx, resourcePath, jobID)
		require.NoError(t, err)

		// Get the job back (should be in ReadyHeap now)
		var retrievedJob *JobMetadata
		retrievedJob, err = manager.GetJobMetadata(ctx, resourcePath, jobID)
		require.NoError(t, err)
		require.NotNil(t, retrievedJob)

		// Linear: attempts=3, backoff calculated with attempts=3, so backoff = 2s * 3 = 6s
		// (attempts is incremented AFTER backoff calculation)
		// Verify attempts were incremented
		assert.Equal(t, int32(4), retrievedJob.Attempts, "Attempts should be incremented from 3 to 4")

		// Verify VisibleAt is set correctly (should be around 6 seconds from when NACK was called)
		assert.True(t, retrievedJob.VisibleAt.After(nowBeforeNACK), "VisibleAt should be after NACK time")
		delay := retrievedJob.VisibleAt.Sub(nowBeforeNACK)
		// Allow reasonable margin: should be around 6 seconds, allow 4-8 seconds
		assert.True(t, delay >= 4*time.Second && delay <= 8*time.Second,
			"Backoff delay should be around 6 seconds (4-8s range), got %v", delay)
	})

	t.Run("Max attempts = 0 (unlimited)", func(t *testing.T) {
		policy := DefaultRetryPolicy()
		policy.MaxAttempts = 0 // Unlimited
		manager.mu.Lock()
		manager.retryPolicies[resourcePath] = &policy
		manager.mu.Unlock()

		jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
		require.NoError(t, err)

		// Manually set very high attempts
		manager.mu.Lock()
		state := manager.queueStates[resourcePath]
		state.mu.Lock()
		job := &JobMetadata{ID: jobID, Attempts: 100}
		state.InFlight[jobID] = job
		state.mu.Unlock()
		manager.mu.Unlock()

		// Should not error even with high attempts
		err = manager.NACK(ctx, resourcePath, jobID)
		assert.NoError(t, err)
	})
}

// TestConcurrentOperations tests concurrent operations
func TestConcurrentOperations(t *testing.T) {
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

	ctx := context.Background()

	t.Run("Concurrent enqueue", func(t *testing.T) {
		const numJobs = 10
		errors := make(chan error, numJobs)

		for i := 0; i < numJobs; i++ {
			go func() {
				_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
				errors <- err
			}()
		}

		for i := 0; i < numJobs; i++ {
			err := <-errors
			assert.NoError(t, err)
		}

		stats, err := manager.GetQueueStats(ctx, resourcePath)
		require.NoError(t, err)
		assert.Equal(t, int64(numJobs), stats.PendingJobs)
	})

	t.Run("Concurrent reserve and ACK", func(t *testing.T) {
		// Enqueue jobs
		const numJobs = 5
		jobIDs := make([]string, numJobs)
		for i := 0; i < numJobs; i++ {
			jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
			require.NoError(t, err)
			jobIDs[i] = jobID
		}

		// Concurrent reserve
		options := DefaultReserveOptions()
		options.VisibilityTimeout = 30 * time.Second

		reserved := make(chan *JobMetadata, numJobs)
		reserveErrors := make(chan error, numJobs)

		for i := 0; i < numJobs; i++ {
			go func() {
				job, err := manager.Reserve(ctx, resourcePath, options)
				if err != nil {
					reserveErrors <- err
					return
				}
				reserved <- job
			}()
		}

		// Collect results
		var jobs []*JobMetadata
		for i := 0; i < numJobs; i++ {
			select {
			case job := <-reserved:
				if job != nil {
					jobs = append(jobs, job)
				}
			case err := <-reserveErrors:
				assert.NoError(t, err)
			}
		}

		// All jobs should be reserved
		assert.Len(t, jobs, numJobs)

		// Concurrent ACK
		ackErrors := make(chan error, numJobs)
		for _, job := range jobs {
			go func(jobID string) {
				err := manager.ACK(ctx, resourcePath, jobID)
				ackErrors <- err
			}(job.ID)
		}

		for i := 0; i < numJobs; i++ {
			err := <-ackErrors
			assert.NoError(t, err)
		}
	})
}
