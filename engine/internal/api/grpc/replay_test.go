package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	replayerrors "github.com/flowmesh/engine/internal/storage/replay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplayService_CreateReplaySession(t *testing.T) {
	t.Run("success with offset", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			createSessionFunc: func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error) {
				return &storage.ReplaySession{
					ID:                   "test-session-id",
					Stream:               stream,
					Partition:            0,
					StartOffset:          startOffset,
					SandboxConsumerGroup: sandboxGroup,
					Status:               "created",
					CreatedAt:            time.Now(),
					UpdatedAt:            time.Now(),
				}, nil
			},
			getReplayProgressFunc: func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
				return &storage.ReplayProgress{
					CurrentOffset:    100,
					MessagesReplayed: 0,
					Errors:           0,
				}, nil
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.CreateReplaySessionRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "stream",
				Name:         "test-stream",
			},
			StartOffset:          100,
			SandboxConsumerGroup: "_replay_test_session_test-group",
		}

		resp, err := service.CreateReplaySession(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.NotNil(t, resp.Session)
		assert.Equal(t, "test-session-id", resp.Session.SessionId)
		assert.Equal(t, int64(100), resp.Session.StartOffset)
	})

	t.Run("success with timestamp", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			createSessionFunc: func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error) {
				return &storage.ReplaySession{
					ID:                   "test-session-id",
					Stream:               stream,
					Partition:            0,
					StartTime:            startTime,
					SandboxConsumerGroup: sandboxGroup,
					Status:               "created",
					CreatedAt:            time.Now(),
					UpdatedAt:            time.Now(),
				}, nil
			},
			getReplayProgressFunc: func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
				return &storage.ReplayProgress{}, nil
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.CreateReplaySessionRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "stream",
				Name:         "test-stream",
			},
			StartTimestamp:       time.Unix(1000, 0).UnixNano(),
			SandboxConsumerGroup: "_replay_test_session_test-group",
		}

		resp, err := service.CreateReplaySession(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.NotNil(t, resp.Session)
		assert.Equal(t, time.Unix(1000, 0).UnixNano(), resp.Session.StartTimestamp)
	})

	t.Run("invalid resource path", func(t *testing.T) {
		service := NewReplayService(&mockReplayStorage{})

		req := &flowmeshpb.CreateReplaySessionRequest{
			ResourcePath: nil,
		}

		_, err := service.CreateReplaySession(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("missing sandbox group", func(t *testing.T) {
		service := NewReplayService(&mockReplayStorage{})

		req := &flowmeshpb.CreateReplaySessionRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "stream",
				Name:         "test-stream",
			},
			StartOffset: 100,
		}

		_, err := service.CreateReplaySession(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("stream not found", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			createSessionFunc: func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error) {
				return nil, replayerrors.StreamNotFoundError{Stream: stream}
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.CreateReplaySessionRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "stream",
				Name:         "nonexistent",
			},
			StartOffset:          100,
			SandboxConsumerGroup: "_replay_test_session_test-group",
		}

		_, err := service.CreateReplaySession(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestReplayService_GetReplaySession(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			getSessionFunc: func(ctx context.Context, sessionID string) (*storage.ReplaySession, error) {
				return &storage.ReplaySession{
					ID:                   sessionID,
					Stream:               "test-stream",
					Partition:            0,
					StartOffset:          100,
					SandboxConsumerGroup: "_replay_test_session_test-group",
					Status:               "active",
					CreatedAt:            time.Now(),
					UpdatedAt:            time.Now(),
				}, nil
			},
			getReplayProgressFunc: func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
				return &storage.ReplayProgress{
					CurrentOffset:    150,
					MessagesReplayed: 50,
					Errors:           0,
				}, nil
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.GetReplaySessionRequest{
			SessionId: "test-session-id",
		}

		resp, err := service.GetReplaySession(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.NotNil(t, resp.Session)
		assert.Equal(t, "test-session-id", resp.Session.SessionId)
		assert.Equal(t, int64(150), resp.Session.Progress.CurrentOffset)
	})

	t.Run("session not found", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			getSessionFunc: func(ctx context.Context, sessionID string) (*storage.ReplaySession, error) {
				return nil, replayerrors.SessionNotFoundError{SessionID: sessionID}
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.GetReplaySessionRequest{
			SessionId: "nonexistent",
		}

		_, err := service.GetReplaySession(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestReplayService_ListReplaySessions(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			listSessionsFunc: func(ctx context.Context, stream string) ([]*storage.ReplaySession, error) {
				return []*storage.ReplaySession{
					{
						ID:                   "session-1",
						Stream:               stream,
						Status:               "active",
						SandboxConsumerGroup: "_replay_test_session_group1",
						CreatedAt:            time.Now(),
						UpdatedAt:            time.Now(),
					},
					{
						ID:                   "session-2",
						Stream:               stream,
						Status:               "paused",
						SandboxConsumerGroup: "_replay_test_session_group2",
						CreatedAt:            time.Now(),
						UpdatedAt:            time.Now(),
					},
				}, nil
			},
			getReplayProgressFunc: func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
				return &storage.ReplayProgress{}, nil
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.ListReplaySessionsRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "stream",
				Name:         "test-stream",
			},
		}

		resp, err := service.ListReplaySessions(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Len(t, resp.Sessions, 2)
	})
}

func TestReplayService_StartReplay(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			startReplayFunc: func(ctx context.Context, sessionID string) error {
				return nil
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.StartReplayRequest{
			SessionId: "test-session-id",
		}

		resp, err := service.StartReplay(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("session not found", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			startReplayFunc: func(ctx context.Context, sessionID string) error {
				return replayerrors.SessionNotFoundError{SessionID: sessionID}
			},
		}
		service := NewReplayService(mockStorage)

		req := &flowmeshpb.StartReplayRequest{
			SessionId: "nonexistent",
		}

		_, err := service.StartReplay(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestReplayService_PauseReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		pauseReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	service := NewReplayService(mockStorage)

	req := &flowmeshpb.PauseReplayRequest{
		SessionId: "test-session-id",
	}

	resp, err := service.PauseReplay(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

func TestReplayService_ResumeReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		resumeReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	service := NewReplayService(mockStorage)

	req := &flowmeshpb.ResumeReplayRequest{
		SessionId: "test-session-id",
	}

	resp, err := service.ResumeReplay(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

func TestReplayService_StopReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		stopReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	service := NewReplayService(mockStorage)

	req := &flowmeshpb.StopReplayRequest{
		SessionId: "test-session-id",
	}

	resp, err := service.StopReplay(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

func TestReplayService_DeleteReplaySession(t *testing.T) {
	mockStorage := &mockReplayStorage{
		deleteSessionFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	service := NewReplayService(mockStorage)

	req := &flowmeshpb.DeleteReplaySessionRequest{
		SessionId: "test-session-id",
	}

	resp, err := service.DeleteReplaySession(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

// Mock implementations

type mockReplayStorage struct {
	storage.StorageBackend
	createSessionFunc     func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error)
	getSessionFunc        func(ctx context.Context, sessionID string) (*storage.ReplaySession, error)
	listSessionsFunc      func(ctx context.Context, stream string) ([]*storage.ReplaySession, error)
	startReplayFunc       func(ctx context.Context, sessionID string) error
	pauseReplayFunc       func(ctx context.Context, sessionID string) error
	resumeReplayFunc      func(ctx context.Context, sessionID string) error
	stopReplayFunc        func(ctx context.Context, sessionID string) error
	deleteSessionFunc     func(ctx context.Context, sessionID string) error
	getReplayProgressFunc func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error)
}

func (m *mockReplayStorage) ReplayManager() storage.ReplayManager {
	return &mockReplayManager{
		createSessionFunc:     m.createSessionFunc,
		getSessionFunc:        m.getSessionFunc,
		listSessionsFunc:      m.listSessionsFunc,
		startReplayFunc:       m.startReplayFunc,
		pauseReplayFunc:       m.pauseReplayFunc,
		resumeReplayFunc:      m.resumeReplayFunc,
		stopReplayFunc:        m.stopReplayFunc,
		deleteSessionFunc:     m.deleteSessionFunc,
		getReplayProgressFunc: m.getReplayProgressFunc,
	}
}

func (m *mockReplayStorage) Ready() bool                     { return true }
func (m *mockReplayStorage) Start(ctx context.Context) error { return nil }
func (m *mockReplayStorage) Stop(ctx context.Context) error  { return nil }

type mockReplayManager struct {
	storage.ReplayManager
	createSessionFunc     func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error)
	getSessionFunc        func(ctx context.Context, sessionID string) (*storage.ReplaySession, error)
	listSessionsFunc      func(ctx context.Context, stream string) ([]*storage.ReplaySession, error)
	startReplayFunc       func(ctx context.Context, sessionID string) error
	pauseReplayFunc       func(ctx context.Context, sessionID string) error
	resumeReplayFunc      func(ctx context.Context, sessionID string) error
	stopReplayFunc        func(ctx context.Context, sessionID string) error
	deleteSessionFunc     func(ctx context.Context, sessionID string) error
	getReplayProgressFunc func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error)
}

func (m *mockReplayManager) CreateSession(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error) {
	if m.createSessionFunc != nil {
		return m.createSessionFunc(ctx, stream, startOffset, startTime, endOffset, endTime, sandboxGroup)
	}
	return nil, nil
}

func (m *mockReplayManager) GetSession(ctx context.Context, sessionID string) (*storage.ReplaySession, error) {
	if m.getSessionFunc != nil {
		return m.getSessionFunc(ctx, sessionID)
	}
	return nil, nil
}

func (m *mockReplayManager) ListSessions(ctx context.Context, stream string) ([]*storage.ReplaySession, error) {
	if m.listSessionsFunc != nil {
		return m.listSessionsFunc(ctx, stream)
	}
	return nil, nil
}

func (m *mockReplayManager) StartReplay(ctx context.Context, sessionID string) error {
	if m.startReplayFunc != nil {
		return m.startReplayFunc(ctx, sessionID)
	}
	return nil
}

func (m *mockReplayManager) PauseReplay(ctx context.Context, sessionID string) error {
	if m.pauseReplayFunc != nil {
		return m.pauseReplayFunc(ctx, sessionID)
	}
	return nil
}

func (m *mockReplayManager) ResumeReplay(ctx context.Context, sessionID string) error {
	if m.resumeReplayFunc != nil {
		return m.resumeReplayFunc(ctx, sessionID)
	}
	return nil
}

func (m *mockReplayManager) StopReplay(ctx context.Context, sessionID string) error {
	if m.stopReplayFunc != nil {
		return m.stopReplayFunc(ctx, sessionID)
	}
	return nil
}

func (m *mockReplayManager) DeleteSession(ctx context.Context, sessionID string) error {
	if m.deleteSessionFunc != nil {
		return m.deleteSessionFunc(ctx, sessionID)
	}
	return nil
}

func (m *mockReplayManager) GetReplayProgress(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
	if m.getReplayProgressFunc != nil {
		return m.getReplayProgressFunc(ctx, sessionID)
	}
	return nil, nil
}

func (m *mockReplayManager) Ready() bool                     { return true }
func (m *mockReplayManager) Start(ctx context.Context) error { return nil }
func (m *mockReplayManager) Stop(ctx context.Context) error  { return nil }
