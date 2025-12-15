package queues

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_ProcessExpiredJobs(t *testing.T) {
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

	// Set retry policy
	policy := DefaultRetryPolicy()
	manager.mu.Lock()
	manager.retryPolicies[resourcePath] = &policy
	manager.mu.Unlock()

	// Enqueue a job
	jobID, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("job"), DefaultEnqueueOptions())
	require.NoError(t, err)

	// Add to InFlight with expired timeout
	manager.mu.Lock()
	state := manager.queueStates[resourcePath]
	state.mu.Lock()
	job := state.InFlight[jobID]
	if job == nil {
		// Manually add to InFlight
		allJobs := state.ReadyHeap.GetAll()
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
	} else {
		job.ReserveUntil = time.Now().Add(-1 * time.Second) // Expired
	}
	state.mu.Unlock()
	manager.mu.Unlock()

	// Process expired jobs via scheduler
	manager.scheduler.processExpiredJobs(resourcePath, state)

	// Job should be back in ReadyHeap (may have backoff delay)
	// Check metadata instead of popping since job may not be visible yet
	ctx := context.Background()
	jobMeta, err := manager.GetJobMetadata(ctx, resourcePath, jobID)
	require.NoError(t, err)
	require.NotNil(t, jobMeta)
	assert.Equal(t, jobID, jobMeta.ID)
	assert.True(t, jobMeta.Attempts > 0)

	// Job should not be in InFlight anymore
	_, err = manager.GetInFlight(resourcePath, jobID)
	assert.Error(t, err)
	assert.IsType(t, JobNotFoundError{}, err)
}

func TestScheduler_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	metaStore, err := metastore.NewStore(filepath.Join(tmpDir, "metadata"))
	require.NoError(t, err)

	logManager := log.NewManager(filepath.Join(tmpDir, "data"))

	scheduler := NewScheduler(metaStore, logManager)

	ctx := context.Background()

	// Start scheduler
	err = scheduler.Start(ctx)
	require.NoError(t, err)

	// Stop scheduler
	err = scheduler.Stop(ctx)
	require.NoError(t, err)
}
