package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/log"
	queueerrors "github.com/flowmesh/engine/internal/storage/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockQueueStorage is a mock implementation of storage.StorageBackend for testing
type mockQueueStorage struct {
	storage.StorageBackend
	queueMgr *mockQueueManager
}

func (m *mockQueueStorage) QueueManager() storage.QueueManager {
	return m.queueMgr
}

func (m *mockQueueStorage) StreamManager() storage.StreamManager {
	return nil
}

func (m *mockQueueStorage) KVManager() storage.KVManager {
	return nil
}

func (m *mockQueueStorage) ConsumerGroupManager() storage.ConsumerGroupManager {
	return nil
}

func (m *mockQueueStorage) Start(ctx context.Context) error {
	return nil
}

func (m *mockQueueStorage) Stop(ctx context.Context) error {
	return nil
}

// mockQueueManager is a mock implementation of storage.QueueManager for testing
type mockQueueManager struct {
	enqueueFunc            func(ctx context.Context, resourcePath string, payload []byte, options storage.QueueEnqueueOptions) (string, int64, error)
	reserveFunc            func(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*storage.QueueJob, error)
	receiveFunc            func(ctx context.Context, resourcePath string, maxJobs int, options storage.QueueReserveOptions) ([]*storage.QueueJob, error)
	removeFromInFlightFunc func(ctx context.Context, resourcePath string, jobID string) error
	nackFunc               func(ctx context.Context, resourcePath string, jobID string) error
	nackWithDelayFunc      func(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error
	getJobPayloadFunc      func(ctx context.Context, resourcePath string, jobID string) ([]byte, error)
	getQueueStatsFunc      func(ctx context.Context, resourcePath string) (*storage.QueueStats, error)
	getInFlightFunc        func(ctx context.Context, resourcePath string, jobID string) (*storage.QueueJob, error)
}

func (m *mockQueueManager) Enqueue(ctx context.Context, resourcePath string, payload []byte, options storage.QueueEnqueueOptions) (string, int64, error) {
	if m.enqueueFunc != nil {
		return m.enqueueFunc(ctx, resourcePath, payload, options)
	}
	return "", 0, nil
}

func (m *mockQueueManager) Reserve(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*storage.QueueJob, error) {
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, resourcePath, visibilityTimeout)
	}
	return nil, nil
}

func (m *mockQueueManager) Receive(ctx context.Context, resourcePath string, maxJobs int, options storage.QueueReserveOptions) ([]*storage.QueueJob, error) {
	if m.receiveFunc != nil {
		return m.receiveFunc(ctx, resourcePath, maxJobs, options)
	}
	return nil, nil
}

func (m *mockQueueManager) RemoveFromInFlight(ctx context.Context, resourcePath string, jobID string) error {
	if m.removeFromInFlightFunc != nil {
		return m.removeFromInFlightFunc(ctx, resourcePath, jobID)
	}
	return nil
}

func (m *mockQueueManager) NACK(ctx context.Context, resourcePath string, jobID string) error {
	if m.nackFunc != nil {
		return m.nackFunc(ctx, resourcePath, jobID)
	}
	return nil
}

func (m *mockQueueManager) NACKWithDelay(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error {
	if m.nackWithDelayFunc != nil {
		return m.nackWithDelayFunc(ctx, resourcePath, jobID, delay)
	}
	return nil
}

func (m *mockQueueManager) GetJobPayload(ctx context.Context, resourcePath string, jobID string) ([]byte, error) {
	if m.getJobPayloadFunc != nil {
		return m.getJobPayloadFunc(ctx, resourcePath, jobID)
	}
	return []byte("test payload"), nil
}

func (m *mockQueueManager) GetQueueStats(ctx context.Context, resourcePath string) (*storage.QueueStats, error) {
	if m.getQueueStatsFunc != nil {
		return m.getQueueStatsFunc(ctx, resourcePath)
	}
	return &storage.QueueStats{}, nil
}

func (m *mockQueueManager) GetInFlight(ctx context.Context, resourcePath string, jobID string) (*storage.QueueJob, error) {
	if m.getInFlightFunc != nil {
		return m.getInFlightFunc(ctx, resourcePath, jobID)
	}
	return nil, queueerrors.JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
}

func (m *mockQueueManager) Start(ctx context.Context) error {
	return nil
}

func (m *mockQueueManager) Stop(ctx context.Context) error {
	return nil
}

func (m *mockQueueManager) InitializeQueue(ctx context.Context, resourcePath string) error {
	return nil
}

func (m *mockQueueManager) AddToInFlight(ctx context.Context, resourcePath string, jobID string, visibilityTimeout time.Duration) (*storage.QueueJob, error) {
	return nil, nil
}

func (m *mockQueueManager) PopReadyJob(ctx context.Context, resourcePath string) (*storage.QueueJob, error) {
	return nil, nil
}

func (m *mockQueueManager) Ready() bool {
	return true
}

func TestQueueService_Enqueue(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			enqueueFunc: func(ctx context.Context, resourcePath string, payload []byte, options storage.QueueEnqueueOptions) (string, int64, error) {
				return "job-123", 1, nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.EnqueueRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			Payload:      []byte("test payload"),
			DelaySeconds: 0,
		}

		resp, err := service.Enqueue(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Equal(t, "job-123", resp.JobId)
		assert.Equal(t, int64(1), resp.Seq)
	})

	t.Run("invalid resource path", func(t *testing.T) {
		service := NewQueueService(&mockQueueStorage{queueMgr: &mockQueueManager{}})

		req := &flowmeshpb.EnqueueRequest{
			ResourcePath: nil,
			Payload:      []byte("test"),
		}

		_, err := service.Enqueue(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("empty payload", func(t *testing.T) {
		service := NewQueueService(&mockQueueStorage{queueMgr: &mockQueueManager{}})

		req := &flowmeshpb.EnqueueRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			Payload: []byte{},
		}

		_, err := service.Enqueue(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("queue not found", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			enqueueFunc: func(ctx context.Context, resourcePath string, payload []byte, options storage.QueueEnqueueOptions) (string, int64, error) {
				return "", 0, queueerrors.QueueNotFoundError{ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.EnqueueRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "nonexistent",
			},
			Payload: []byte("test"),
		}

		_, err := service.Enqueue(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestQueueService_Reserve(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		now := time.Now()
		mockMgr := &mockQueueManager{
			reserveFunc: func(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*storage.QueueJob, error) {
				return &storage.QueueJob{
					ID:           "job-123",
					Seq:          1,
					CreatedAt:    now,
					VisibleAt:    now,
					ReserveUntil: now.Add(30 * time.Second),
					Attempts:     0,
					PayloadPos:   log.FilePointer{File: "/test/path", Offset: 0},
				}, nil
			},
			getJobPayloadFunc: func(ctx context.Context, resourcePath string, jobID string) ([]byte, error) {
				return []byte("test payload"), nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.ReserveRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			VisibilityTimeoutSeconds: 30,
		}

		resp, err := service.Reserve(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.NotNil(t, resp.Job)
		assert.Equal(t, "job-123", resp.Job.Id)
	})

	t.Run("no jobs available", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			reserveFunc: func(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*storage.QueueJob, error) {
				return nil, nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.ReserveRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
		}

		resp, err := service.Reserve(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Nil(t, resp.Job)
	})

	t.Run("queue not found", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			reserveFunc: func(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*storage.QueueJob, error) {
				return nil, queueerrors.QueueNotFoundError{ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.ReserveRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "nonexistent",
			},
		}

		_, err := service.Reserve(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestQueueService_ACK(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			removeFromInFlightFunc: func(ctx context.Context, resourcePath string, jobID string) error {
				return nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.ACKRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			JobId: "job-123",
		}

		resp, err := service.ACK(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("job not found", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			removeFromInFlightFunc: func(ctx context.Context, resourcePath string, jobID string) error {
				return queueerrors.JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.ACKRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			JobId: "nonexistent",
		}

		_, err := service.ACK(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestQueueService_NACK(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			nackFunc: func(ctx context.Context, resourcePath string, jobID string) error {
				return nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.NACKRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			JobId: "job-123",
		}

		resp, err := service.NACK(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("with delay", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			nackWithDelayFunc: func(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error {
				return nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.NACKRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
			JobId:        "job-123",
			DelaySeconds: 60,
		}

		resp, err := service.NACK(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})
}

func TestQueueService_GetQueueStats(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			getQueueStatsFunc: func(ctx context.Context, resourcePath string) (*storage.QueueStats, error) {
				return &storage.QueueStats{
					TotalJobs:     100,
					PendingJobs:   50,
					InFlightJobs:  10,
					CompletedJobs: 30,
					FailedJobs:    10,
					OldestJobAge:  5 * time.Minute,
				}, nil
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.GetQueueStatsRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "test-queue",
			},
		}

		resp, err := service.GetQueueStats(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.NotNil(t, resp.Stats)
		assert.Equal(t, int64(100), resp.Stats.TotalJobs)
		assert.Equal(t, int64(50), resp.Stats.PendingJobs)
	})

	t.Run("queue not found", func(t *testing.T) {
		mockMgr := &mockQueueManager{
			getQueueStatsFunc: func(ctx context.Context, resourcePath string) (*storage.QueueStats, error) {
				return nil, queueerrors.QueueNotFoundError{ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockQueueStorage{queueMgr: mockMgr}
		service := NewQueueService(mockStorage)

		req := &flowmeshpb.GetQueueStatsRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "queue",
				Name:         "nonexistent",
			},
		}

		_, err := service.GetQueueStats(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}
