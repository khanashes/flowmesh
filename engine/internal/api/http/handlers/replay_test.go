package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage"
	replayerrors "github.com/flowmesh/engine/internal/storage/replay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayHandlers_CreateReplaySession(t *testing.T) {
	t.Run("success", func(t *testing.T) {
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
		handlers := NewReplayHandlers(mockStorage)

		reqBody := CreateReplaySessionRequest{
			StartOffset:          intPtr(100),
			SandboxConsumerGroup: "_replay_test_session_test-group",
		}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/replay/sessions?stream=test-tenant/test-ns/test-stream", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.CreateReplaySession(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var resp ReplaySessionResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "test-session-id", resp.ID)
		assert.Equal(t, int64(100), resp.StartOffset)
	})

	t.Run("missing stream parameter", func(t *testing.T) {
		handlers := NewReplayHandlers(&mockReplayStorage{})

		reqBody := CreateReplaySessionRequest{
			StartOffset:          intPtr(100),
			SandboxConsumerGroup: "_replay_test_session_test-group",
		}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/replay/sessions", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.CreateReplaySession(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid sandbox group", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			createSessionFunc: func(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*storage.ReplaySession, error) {
				// Return validation error for invalid sandbox group
				return nil, fmt.Errorf("sandbox consumer group name must start with _replay_")
			},
		}
		handlers := NewReplayHandlers(mockStorage)

		reqBody := CreateReplaySessionRequest{
			StartOffset:          intPtr(100),
			SandboxConsumerGroup: "invalid-group",
		}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/replay/sessions?stream=test-tenant/test-ns/test-stream", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.CreateReplaySession(w, req)

		// Should return 400 Bad Request for validation error
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "sandbox consumer group")
	})
}

func TestReplayHandlers_ListReplaySessions(t *testing.T) {
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
				}, nil
			},
			getReplayProgressFunc: func(ctx context.Context, sessionID string) (*storage.ReplayProgress, error) {
				return &storage.ReplayProgress{}, nil
			},
		}
		handlers := NewReplayHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/replay/sessions", nil)
		w := httptest.NewRecorder()

		handlers.ListReplaySessions(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp["status"])
	})
}

func TestReplayHandlers_GetReplaySession(t *testing.T) {
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
		handlers := NewReplayHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/replay/sessions/test-session-id", nil)
		w := httptest.NewRecorder()

		handlers.GetReplaySession(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp ReplaySessionResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "test-session-id", resp.ID)
		assert.Equal(t, int64(150), resp.Progress.CurrentOffset)
	})

	t.Run("session not found", func(t *testing.T) {
		mockStorage := &mockReplayStorage{
			getSessionFunc: func(ctx context.Context, sessionID string) (*storage.ReplaySession, error) {
				return nil, replayerrors.SessionNotFoundError{SessionID: sessionID}
			},
		}
		handlers := NewReplayHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/replay/sessions/nonexistent", nil)
		w := httptest.NewRecorder()

		handlers.GetReplaySession(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestReplayHandlers_StartReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		startReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	handlers := NewReplayHandlers(mockStorage)

	req := httptest.NewRequest("POST", "/api/v1/replay/sessions/test-session-id/start", nil)
	w := httptest.NewRecorder()

	handlers.StartReplay(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "success", resp["status"])
}

func TestReplayHandlers_PauseReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		pauseReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	handlers := NewReplayHandlers(mockStorage)

	req := httptest.NewRequest("POST", "/api/v1/replay/sessions/test-session-id/pause", nil)
	w := httptest.NewRecorder()

	handlers.PauseReplay(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReplayHandlers_ResumeReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		resumeReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	handlers := NewReplayHandlers(mockStorage)

	req := httptest.NewRequest("POST", "/api/v1/replay/sessions/test-session-id/resume", nil)
	w := httptest.NewRecorder()

	handlers.ResumeReplay(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReplayHandlers_StopReplay(t *testing.T) {
	mockStorage := &mockReplayStorage{
		stopReplayFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	handlers := NewReplayHandlers(mockStorage)

	req := httptest.NewRequest("POST", "/api/v1/replay/sessions/test-session-id/stop", nil)
	w := httptest.NewRecorder()

	handlers.StopReplay(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReplayHandlers_DeleteReplaySession(t *testing.T) {
	mockStorage := &mockReplayStorage{
		deleteSessionFunc: func(ctx context.Context, sessionID string) error {
			return nil
		},
	}
	handlers := NewReplayHandlers(mockStorage)

	req := httptest.NewRequest("DELETE", "/api/v1/replay/sessions/test-session-id", nil)
	w := httptest.NewRecorder()

	handlers.DeleteReplaySession(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
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

// Helper function
func intPtr(i int64) *int64 {
	return &i
}
