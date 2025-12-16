package replay

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

func setupTestManager(t *testing.T) (*Manager, *metastore.Store, string, *Executor, func()) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	// Create a mock executor
	mockStreamReader := &mockStreamReader{}
	mockConsumerMgr := &mockConsumerGroupManager{}
	executor := NewExecutor(nil, mockStreamReader, mockConsumerMgr)

	manager := NewManager(metaStore, metadataDir, executor)
	executor.SetManager(manager)

	ctx := context.Background()
	err = manager.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		err := manager.Stop(ctx)
		assert.NoError(t, err)
	}

	return manager, metaStore, tmpDir, executor, cleanup
}

func createTestStream(t *testing.T, metaStore *metastore.Store, name string) string {
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       name,
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	return resourcePath
}

// Mock implementations for testing
type mockStreamReader struct {
	readFromOffsetFunc        func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error)
	findOffsetByTimestampFunc func(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error)
}

func (m *mockStreamReader) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	if m.readFromOffsetFunc != nil {
		return m.readFromOffsetFunc(ctx, resourcePath, partition, offset, maxMessages)
	}
	return []*log.Message{}, nil
}

func (m *mockStreamReader) FindOffsetByTimestamp(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error) {
	if m.findOffsetByTimestampFunc != nil {
		return m.findOffsetByTimestampFunc(ctx, resourcePath, partition, timestamp)
	}
	return 0, nil
}

type mockConsumerGroupManager struct {
	commitOffsetFunc func(ctx context.Context, stream, group string, partition int32, offset int64) error
}

func (m *mockConsumerGroupManager) CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error {
	if m.commitOffsetFunc != nil {
		return m.commitOffsetFunc(ctx, stream, group, partition, offset)
	}
	return nil
}

func TestManager_CreateSession(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, session.ID)
	assert.Equal(t, stream, session.Stream)
	assert.Equal(t, int64(100), session.StartOffset)
	assert.Equal(t, SessionStatusCreated, session.Status)
	assert.Equal(t, "_replay_test_session_test-group", session.SandboxConsumerGroup)
}

func TestManager_CreateSession_WithTimestamp(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	startTime := time.Unix(1000, 0)
	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartTime:            &startTime,
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, session.ID)
	assert.NotNil(t, session.StartTime)
	assert.Equal(t, startTime, *session.StartTime)
}

func TestManager_CreateSession_InvalidSandboxGroup(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "invalid-group", // Missing prefix
	}

	_, err := manager.CreateSessionWithRequest(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sandbox consumer group name must start with")
}

func TestManager_CreateSession_StreamNotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	req := StartRequest{
		Stream:               "nonexistent/stream",
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	_, err := manager.CreateSessionWithRequest(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream not found")
}

func TestManager_GetSession(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Get the session
	retrieved, err := manager.GetSession(ctx, session.ID)
	require.NoError(t, err)
	assert.Equal(t, session.ID, retrieved.ID)
	assert.Equal(t, session.Stream, retrieved.Stream)
}

func TestManager_GetSession_NotFound(t *testing.T) {
	manager, _, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	_, err := manager.GetSession(ctx, "nonexistent-session-id")
	assert.Error(t, err)
	assert.IsType(t, SessionNotFoundError{}, err)
}

func TestManager_ListSessions(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream1 := createTestStream(t, metaStore, "test-stream-1")
	stream2 := createTestStream(t, metaStore, "test-stream-2")

	// Create sessions for different streams
	req1 := StartRequest{
		Stream:               stream1,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_group1",
	}
	session1, err := manager.CreateSessionWithRequest(ctx, req1)
	require.NoError(t, err)

	req2 := StartRequest{
		Stream:               stream2,
		Partition:            0,
		StartOffset:          intPtr(200),
		SandboxConsumerGroup: "_replay_test_session_group2",
	}
	_, err = manager.CreateSessionWithRequest(ctx, req2)
	require.NoError(t, err)

	// List all sessions
	allSessions, err := manager.ListSessions(ctx, "")
	require.NoError(t, err)
	assert.Len(t, allSessions, 2)

	// List sessions for specific stream
	stream1Sessions, err := manager.ListSessions(ctx, stream1)
	require.NoError(t, err)
	assert.Len(t, stream1Sessions, 1)
	assert.Equal(t, session1.ID, stream1Sessions[0].ID)
}

func TestManager_UpdateSessionStatus(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Update status to active
	err = manager.UpdateSessionStatus(ctx, session.ID, SessionStatusActive)
	require.NoError(t, err)

	// Verify status was updated
	updated, err := manager.GetSession(ctx, session.ID)
	require.NoError(t, err)
	assert.Equal(t, SessionStatusActive, updated.Status)
}

func TestManager_UpdateSessionStatus_InvalidTransition(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Transition from created -> active (valid)
	err = manager.UpdateSessionStatus(ctx, session.ID, SessionStatusActive)
	require.NoError(t, err) // created -> active is valid

	// Now stop it
	err = manager.UpdateSessionStatus(ctx, session.ID, SessionStatusStopped)
	require.NoError(t, err)

	// Try to go from stopped to active (invalid)
	err = manager.UpdateSessionStatus(ctx, session.ID, SessionStatusActive)
	assert.Error(t, err)
	assert.IsType(t, InvalidSessionStateError{}, err)
}

func TestManager_DeleteSession(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Delete the session
	err = manager.DeleteSession(ctx, session.ID)
	require.NoError(t, err)

	// Verify session is gone
	_, err = manager.GetSession(ctx, session.ID)
	assert.Error(t, err)
	assert.IsType(t, SessionNotFoundError{}, err)
}

func TestManager_GetReplayProgress(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Get progress
	progress, err := manager.GetReplayProgress(ctx, session.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(100), progress.CurrentOffset)
	assert.Equal(t, int64(0), progress.MessagesReplayed)
	assert.Equal(t, int64(0), progress.Errors)
}

func TestManager_UpdateProgress(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)

	// Update progress
	newProgress := &Progress{
		CurrentOffset:    200,
		MessagesReplayed: 50,
		Errors:           2,
	}
	err = manager.UpdateProgress(ctx, session.ID, newProgress)
	require.NoError(t, err)

	// Verify progress was updated
	progress, err := manager.GetReplayProgress(ctx, session.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(200), progress.CurrentOffset)
	assert.Equal(t, int64(50), progress.MessagesReplayed)
	assert.Equal(t, int64(2), progress.Errors)
}

func TestManager_CreateSession_WithEndOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(100),
		EndOffset:            intPtr(500),
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	session, err := manager.CreateSessionWithRequest(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, session.EndOffset)
	assert.Equal(t, int64(500), *session.EndOffset)
}

func TestManager_CreateSession_InvalidEndOffset(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	stream := createTestStream(t, metaStore, "test-stream")

	req := StartRequest{
		Stream:               stream,
		Partition:            0,
		StartOffset:          intPtr(500),
		EndOffset:            intPtr(100), // Less than start
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	_, err := manager.CreateSessionWithRequest(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "end offset must be >= start offset")
}

// Helper function
func intPtr(i int64) *int64 {
	return &i
}
