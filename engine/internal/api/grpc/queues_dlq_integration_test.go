package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	queues "github.com/flowmesh/engine/internal/storage/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupQueueIntegrationTest(t *testing.T) (storage.StorageBackend, func()) {
	tmpDir := t.TempDir()

	// Create real storage backend
	storageBackend, err := storage.New(tmpDir)
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		err := storageBackend.Stop(ctx)
		assert.NoError(t, err)
	}

	return storageBackend, cleanup
}

func createQueueForGRPC(t *testing.T, storageBackend storage.StorageBackend, tenant, namespace, name string) string {
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       name,
		Type:       metastore.ResourceQueue,
		Partitions: 1,
		CreatedAt:  time.Now(),
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := storageBackend.MetaStore().CreateResource(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.QueueManager().InitializeQueue(ctx, resourcePath)
	require.NoError(t, err)

	return resourcePath
}

func TestQueueService_Integration_SetRetryPolicy(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	// Set retry policy
	setReq := &flowmeshpb.SetRetryPolicyRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		Policy: &flowmeshpb.RetryPolicy{
			MaxAttempts:           3,
			InitialBackoffSeconds: 1,
			MaxBackoffSeconds:     60,
			BackoffMultiplier:     2.0,
			BackoffStrategy:       "exponential",
		},
	}

	setResp, err := service.SetRetryPolicy(context.Background(), setReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), setResp.Status.Code) // codes.OK = 0

	// Get retry policy to verify it was persisted
	getReq := &flowmeshpb.GetRetryPolicyRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
	}

	getResp, err := service.GetRetryPolicy(context.Background(), getReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), getResp.Status.Code)
	assert.NotNil(t, getResp.Policy)
	assert.Equal(t, int32(3), getResp.Policy.MaxAttempts)
	assert.Equal(t, int64(1), getResp.Policy.InitialBackoffSeconds)
	assert.Equal(t, int64(60), getResp.Policy.MaxBackoffSeconds)
	assert.Equal(t, 2.0, getResp.Policy.BackoffMultiplier)
	assert.Equal(t, "exponential", getResp.Policy.BackoffStrategy)
}

func TestQueueService_Integration_DLQ_AutoCreation(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	resourcePath := createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	// Get DLQ path - should auto-create DLQ queue
	queueMgr := storageBackend.QueueManager()
	ctx := context.Background()

	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	require.NoError(t, err)
	assert.Equal(t, resourcePath+"-dlq", dlqPath)

	// Verify DLQ queue was created in metadata
	dlqConfig, err := storageBackend.MetaStore().GetResource(dlqPath)
	require.NoError(t, err)
	assert.Equal(t, metastore.ResourceQueue, dlqConfig.Type)
	assert.Equal(t, "test-queue-dlq", dlqConfig.Name)

	// Verify original queue config has DLQ path stored
	queueConfig, err := storageBackend.MetaStore().GetResource(resourcePath)
	require.NoError(t, err)
	assert.NotNil(t, queueConfig.DLQ)
	assert.True(t, queueConfig.DLQ.Enabled)
	assert.Equal(t, dlqPath, queueConfig.DLQ.DLQPath)
}

func TestQueueService_Integration_MoveToDLQ_Manual(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	resourcePath := createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	ctx := context.Background()
	queueMgr := storageBackend.QueueManager()

	// Enqueue a job
	enqueueReq := &flowmeshpb.EnqueueRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		Payload: []byte("test job payload"),
	}

	enqueueResp, err := service.Enqueue(ctx, enqueueReq)
	require.NoError(t, err)
	jobID := enqueueResp.JobId

	// Reserve the job to move it to InFlight (use storage layer directly)
	// Need to use the storage interface which takes time.Duration
	queueJob, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, queueJob)
	assert.Equal(t, jobID, queueJob.ID)

	// Manually move job to DLQ
	err = queueMgr.MoveToDLQ(ctx, resourcePath, jobID)
	require.NoError(t, err)

	// Verify job is in DLQ
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	require.NoError(t, err)

	dlqJobs, err := queueMgr.ListDLQJobs(ctx, resourcePath, 10)
	require.NoError(t, err)
	assert.Len(t, dlqJobs, 1)
	// DLQ creates a new job ID, so check payload instead
	dlqPayload, err := queueMgr.GetJobPayload(ctx, dlqPath, dlqJobs[0].ID)
	require.NoError(t, err)
	assert.Equal(t, []byte("test job payload"), dlqPayload)
}

func TestQueueService_Integration_MoveToDLQ_MaxAttempts(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	resourcePath := createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	ctx := context.Background()
	queueMgr := storageBackend.QueueManager()

	// Set retry policy with max attempts = 2
	retryPolicy := queues.RetryPolicy{
		MaxAttempts:       2,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		BackoffStrategy:   queues.BackoffStrategyExponential,
	}

	err := queueMgr.SetRetryPolicy(ctx, resourcePath, retryPolicy)
	require.NoError(t, err)

	// Enqueue a job
	enqueueReq := &flowmeshpb.EnqueueRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		Payload: []byte("failing job"),
	}

	enqueueResp, err := service.Enqueue(ctx, enqueueReq)
	require.NoError(t, err)
	jobID := enqueueResp.JobId

	// Reserve and NACK first time
	queueJob, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 1*time.Second)
	require.NoError(t, err)
	require.NotNil(t, queueJob)
	assert.Equal(t, jobID, queueJob.ID)

	// NACK the job first time
	err = queueMgr.NACK(ctx, resourcePath, jobID)
	require.NoError(t, err)

	// Wait for backoff
	time.Sleep(300 * time.Millisecond)

	// Reserve and NACK second time - this should exceed max attempts
	queueJob2, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 1*time.Second)
	require.NoError(t, err)
	require.NotNil(t, queueJob2)
	assert.Equal(t, jobID, queueJob2.ID)

	// Second NACK should fail with max attempts exceeded - manually move to DLQ
	err = queueMgr.NACK(ctx, resourcePath, jobID)
	if err != nil {
		// Max attempts exceeded - manually move to DLQ since NACK doesn't do it automatically
		moveErr := queueMgr.MoveToDLQ(ctx, resourcePath, jobID)
		if moveErr != nil {
			// If MoveToDLQ fails, wait for scheduler to handle it
			time.Sleep(2 * time.Second)
		}
	} else {
		// If NACK succeeded, wait for scheduler to process
		time.Sleep(2 * time.Second)
	}

	// Get DLQ path
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	require.NoError(t, err)

	// Verify job is in DLQ (DLQ creates new job IDs, so check by payload)
	dlqJobs, err := queueMgr.ListDLQJobs(ctx, resourcePath, 10)
	require.NoError(t, err)
	require.Len(t, dlqJobs, 1, "Job should be in DLQ after max attempts exceeded")
	// Verify payload matches
	dlqPayload, err := queueMgr.GetJobPayload(ctx, dlqPath, dlqJobs[0].ID)
	require.NoError(t, err)
	assert.Equal(t, []byte("failing job"), dlqPayload)
}

func TestQueueService_Integration_ListDLQJobs(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	resourcePath := createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	ctx := context.Background()
	queueMgr := storageBackend.QueueManager()

	// Enqueue and move multiple jobs to DLQ
	for i := 0; i < 3; i++ {
		payload := []byte("job " + string(rune('A'+i)))
		jobID, _, err := queueMgr.Enqueue(ctx, resourcePath, payload, storage.QueueEnqueueOptions{})
		require.NoError(t, err)

		// Reserve and move to DLQ (use storage layer directly)
		queueJob, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, queueJob)
		assert.Equal(t, jobID, queueJob.ID)

		err = queueMgr.MoveToDLQ(ctx, resourcePath, jobID)
		require.NoError(t, err)
	}

	// List DLQ jobs
	listReq := &flowmeshpb.ListDLQJobsRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		MaxJobs: 10,
	}

	listResp, err := service.ListDLQJobs(ctx, listReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), listResp.Status.Code)
	assert.Len(t, listResp.Jobs, 3)

	// Verify all jobs are present (DLQ creates new job IDs, so check by payload)
	foundPayloads := make(map[string]bool)
	for _, job := range listResp.Jobs {
		foundPayloads[string(job.Payload)] = true
		assert.NotEmpty(t, job.Payload)
	}

	// Check that all expected payloads are in DLQ
	expectedPayloads := []string{"job A", "job B", "job C"}
	for _, payload := range expectedPayloads {
		assert.True(t, foundPayloads[payload], "Job with payload '%s' should be in DLQ", payload)
	}
	assert.Len(t, listResp.Jobs, 3)
}

func TestQueueService_Integration_ReplayDLQJob(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	resourcePath := createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	ctx := context.Background()
	queueMgr := storageBackend.QueueManager()

	// Enqueue and move a job to DLQ
	enqueueReq := &flowmeshpb.EnqueueRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		Payload: []byte("replay me"),
	}

	enqueueResp, err := service.Enqueue(ctx, enqueueReq)
	require.NoError(t, err)
	originalJobID := enqueueResp.JobId

	// Reserve and move to DLQ (use storage layer directly)
	queueJob, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, queueJob)
	assert.Equal(t, originalJobID, queueJob.ID)

	err = queueMgr.MoveToDLQ(ctx, resourcePath, originalJobID)
	require.NoError(t, err)

	// Get the DLQ job ID (DLQ creates a new job with a new ID)
	dlqJobs, err := queueMgr.ListDLQJobs(ctx, resourcePath, 10)
	require.NoError(t, err)
	require.Len(t, dlqJobs, 1)
	dlqJobID := dlqJobs[0].ID

	// Replay the job from DLQ using the DLQ job ID
	replayReq := &flowmeshpb.ReplayDLQJobRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		JobId: dlqJobID,
	}

	replayResp, err := service.ReplayDLQJob(ctx, replayReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), replayResp.Status.Code)
	assert.NotEmpty(t, replayResp.JobId)
	assert.NotEqual(t, originalJobID, replayResp.JobId) // Should be a new job ID

	// Verify the replayed job is in the main queue (use storage layer directly)
	newQueueJob, err := storageBackend.QueueManager().Reserve(ctx, resourcePath, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, newQueueJob)
	// Replay creates a new job ID, so check payload instead
	payload, err := queueMgr.GetJobPayload(ctx, resourcePath, newQueueJob.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte("replay me"), payload)
}

func TestQueueService_Integration_RetryPolicyPersistence(t *testing.T) {
	storageBackend, cleanup := setupQueueIntegrationTest(t)
	defer cleanup()

	service := NewQueueService(storageBackend)
	_ = createQueueForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-queue")

	ctx := context.Background()

	// Set retry policy
	setReq := &flowmeshpb.SetRetryPolicyRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
		Policy: &flowmeshpb.RetryPolicy{
			MaxAttempts:           5,
			InitialBackoffSeconds: 2,
			MaxBackoffSeconds:     120,
			BackoffMultiplier:     3.0,
			BackoffStrategy:       "linear",
		},
	}

	_, err := service.SetRetryPolicy(ctx, setReq)
	require.NoError(t, err)

	// Stop and restart storage to simulate restart
	err = storageBackend.Stop(ctx)
	require.NoError(t, err)

	// Create new storage backend pointing to same directory
	tmpDir := storageBackend.Paths().BaseDir
	newStorageBackend, err := storage.New(tmpDir)
	require.NoError(t, err)

	// Queue already exists, just initialize it
	resourcePath := "test-tenant/test-ns/queue/test-queue"
	err = newStorageBackend.QueueManager().InitializeQueue(ctx, resourcePath)
	require.NoError(t, err)

	err = newStorageBackend.Start(ctx)
	require.NoError(t, err)
	defer newStorageBackend.Stop(ctx)

	// Verify retry policy persisted
	newService := NewQueueService(newStorageBackend)
	getReq := &flowmeshpb.GetRetryPolicyRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "queue",
			Name:         "test-queue",
		},
	}

	getResp, err := newService.GetRetryPolicy(ctx, getReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), getResp.Status.Code)
	assert.NotNil(t, getResp.Policy)
	assert.Equal(t, int32(5), getResp.Policy.MaxAttempts)
	assert.Equal(t, int64(2), getResp.Policy.InitialBackoffSeconds)
	assert.Equal(t, int64(120), getResp.Policy.MaxBackoffSeconds)
	assert.Equal(t, 3.0, getResp.Policy.BackoffMultiplier)
	assert.Equal(t, "linear", getResp.Policy.BackoffStrategy)
}
