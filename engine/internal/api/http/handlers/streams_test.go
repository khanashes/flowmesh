package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamHandlers_WriteEvents(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			writeEventsFunc: func(ctx context.Context, resourcePath string, events []storage.StreamEvent) ([]int64, error) {
				return []int64{0, 1}, nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		reqBody := WriteEventsRequest{
			Events: []Event{
				{Payload: []byte("event1")},
				{Payload: []byte("event2")},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/streams/test-tenant/test-ns/test-stream/events", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.WriteEvents(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp WriteEventsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
		assert.Len(t, resp.Offsets, 2)
	})

	t.Run("invalid path", func(t *testing.T) {
		handlers := NewStreamHandlers(&mockStreamStorage{})

		req := httptest.NewRequest("POST", "/api/v1/streams/invalid", nil)
		w := httptest.NewRecorder()

		handlers.WriteEvents(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestStreamHandlers_ReadStream(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			readFromOffsetFunc: func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
				return []*log.Message{
					{
						ID:           "msg1",
						ResourcePath: resourcePath,
						Partition:    partition,
						Offset:       offset,
						Payload:      []byte("payload1"),
					},
				}, nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/streams/test-tenant/test-ns/test-stream/messages?offset=0&max_messages=10", nil)
		w := httptest.NewRecorder()

		handlers.ReadStream(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp ReadStreamResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
		assert.Len(t, resp.Messages, 1)
	})

	t.Run("missing offset", func(t *testing.T) {
		handlers := NewStreamHandlers(&mockStreamStorage{})

		req := httptest.NewRequest("GET", "/api/v1/streams/test-tenant/test-ns/test-stream/messages", nil)
		w := httptest.NewRecorder()

		handlers.ReadStream(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestStreamHandlers_CommitOffset(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			commitOffsetFunc: func(ctx context.Context, stream, group string, partition int32, offset int64) error {
				return nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		reqBody := CommitOffsetRequest{Offset: 100}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/streams/test-tenant/test-ns/test-stream/consumer-groups/test-group/offsets", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.CommitOffset(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp CommitOffsetResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
	})
}

func TestStreamHandlers_GetOffset(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			getCommittedOffsetFunc: func(ctx context.Context, stream, group string, partition int32) (int64, error) {
				return 100, nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/streams/test-tenant/test-ns/test-stream/consumer-groups/test-group/offsets", nil)
		w := httptest.NewRecorder()

		handlers.GetOffset(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp GetOffsetResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
		assert.Equal(t, int64(100), resp.Offset)
	})
}

func TestStreamHandlers_GetLatestOffset(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			getLatestOffsetFunc: func(ctx context.Context, resourcePath string, partition int32) (int64, error) {
				return 200, nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/streams/test-tenant/test-ns/test-stream/offsets/latest", nil)
		w := httptest.NewRecorder()

		handlers.GetLatestOffset(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp GetLatestOffsetResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
		assert.Equal(t, int64(200), resp.Offset)
	})
}

func TestStreamHandlers_GetConsumerGroupState(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockStorage := &mockStreamStorage{
			getConsumerGroupStateFunc: func(ctx context.Context, stream, group string, partition int32) (*storage.ConsumerGroupState, error) {
				return &storage.ConsumerGroupState{
					Stream:          stream,
					Group:           group,
					Partition:       partition,
					CommittedOffset: 100,
					LatestOffset:    200,
					Lag:             100,
				}, nil
			},
		}
		handlers := NewStreamHandlers(mockStorage)

		req := httptest.NewRequest("GET", "/api/v1/streams/test-tenant/test-ns/test-stream/consumer-groups/test-group/state", nil)
		w := httptest.NewRecorder()

		handlers.GetConsumerGroupState(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp GetConsumerGroupStateResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "success", resp.Status)
		assert.Equal(t, int64(100), resp.State.CommittedOffset)
		assert.Equal(t, int64(200), resp.State.LatestOffset)
		assert.Equal(t, int64(100), resp.State.Lag)
	})
}

// Mock implementations

type mockStreamStorage struct {
	storage.StorageBackend
	writeEventsFunc           func(ctx context.Context, resourcePath string, events []storage.StreamEvent) ([]int64, error)
	readFromOffsetFunc        func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error)
	getLatestOffsetFunc       func(ctx context.Context, resourcePath string, partition int32) (int64, error)
	commitOffsetFunc          func(ctx context.Context, stream, group string, partition int32, offset int64) error
	getCommittedOffsetFunc    func(ctx context.Context, stream, group string, partition int32) (int64, error)
	getConsumerGroupStateFunc func(ctx context.Context, stream, group string, partition int32) (*storage.ConsumerGroupState, error)
}

func (m *mockStreamStorage) StreamManager() storage.StreamManager {
	return &mockStreamManager{
		writeEventsFunc:     m.writeEventsFunc,
		readFromOffsetFunc:  m.readFromOffsetFunc,
		getLatestOffsetFunc: m.getLatestOffsetFunc,
	}
}

func (m *mockStreamStorage) ConsumerGroupManager() storage.ConsumerGroupManager {
	return &mockConsumerGroupManager{
		commitOffsetFunc:          m.commitOffsetFunc,
		getCommittedOffsetFunc:    m.getCommittedOffsetFunc,
		getConsumerGroupStateFunc: m.getConsumerGroupStateFunc,
	}
}

func (m *mockStreamStorage) Ready() bool                     { return true }
func (m *mockStreamStorage) Start(ctx context.Context) error { return nil }
func (m *mockStreamStorage) Stop(ctx context.Context) error  { return nil }

type mockStreamManager struct {
	storage.StreamManager
	writeEventsFunc     func(ctx context.Context, resourcePath string, events []storage.StreamEvent) ([]int64, error)
	readFromOffsetFunc  func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error)
	getLatestOffsetFunc func(ctx context.Context, resourcePath string, partition int32) (int64, error)
}

func (m *mockStreamManager) WriteEvents(ctx context.Context, resourcePath string, events []storage.StreamEvent) ([]int64, error) {
	if m.writeEventsFunc != nil {
		return m.writeEventsFunc(ctx, resourcePath, events)
	}
	return nil, nil
}

func (m *mockStreamManager) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	if m.readFromOffsetFunc != nil {
		return m.readFromOffsetFunc(ctx, resourcePath, partition, offset, maxMessages)
	}
	return nil, nil
}

func (m *mockStreamManager) GetLatestOffset(ctx context.Context, resourcePath string, partition int32) (int64, error) {
	if m.getLatestOffsetFunc != nil {
		return m.getLatestOffsetFunc(ctx, resourcePath, partition)
	}
	return -1, nil
}

func (m *mockStreamManager) Ready() bool                     { return true }
func (m *mockStreamManager) Start(ctx context.Context) error { return nil }
func (m *mockStreamManager) Stop(ctx context.Context) error  { return nil }
func (m *mockStreamManager) InitializeStream(ctx context.Context, resourcePath string) error {
	return nil
}

type mockConsumerGroupManager struct {
	storage.ConsumerGroupManager
	commitOffsetFunc          func(ctx context.Context, stream, group string, partition int32, offset int64) error
	getCommittedOffsetFunc    func(ctx context.Context, stream, group string, partition int32) (int64, error)
	getConsumerGroupStateFunc func(ctx context.Context, stream, group string, partition int32) (*storage.ConsumerGroupState, error)
}

func (m *mockConsumerGroupManager) CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error {
	if m.commitOffsetFunc != nil {
		return m.commitOffsetFunc(ctx, stream, group, partition, offset)
	}
	return nil
}

func (m *mockConsumerGroupManager) GetCommittedOffset(ctx context.Context, stream, group string, partition int32) (int64, error) {
	if m.getCommittedOffsetFunc != nil {
		return m.getCommittedOffsetFunc(ctx, stream, group, partition)
	}
	return -1, nil
}

func (m *mockConsumerGroupManager) GetConsumerGroupState(ctx context.Context, stream, group string, partition int32) (*storage.ConsumerGroupState, error) {
	if m.getConsumerGroupStateFunc != nil {
		return m.getConsumerGroupStateFunc(ctx, stream, group, partition)
	}
	return nil, nil
}

func (m *mockConsumerGroupManager) Ready() bool                     { return true }
func (m *mockConsumerGroupManager) Start(ctx context.Context) error { return nil }
func (m *mockConsumerGroupManager) Stop(ctx context.Context) error  { return nil }
func (m *mockConsumerGroupManager) CalculateLag(ctx context.Context, stream, group string, partition int32) (int64, error) {
	return 0, nil
}
func (m *mockConsumerGroupManager) ListConsumerGroups(ctx context.Context, stream string) ([]string, error) {
	return nil, nil
}
func (m *mockConsumerGroupManager) DeleteConsumerGroup(ctx context.Context, stream, group string) error {
	return nil
}
