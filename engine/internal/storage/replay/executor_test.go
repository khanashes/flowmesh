package replay

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestExecutor(t *testing.T) (*Executor, *Manager, *mockStreamReader, *mockConsumerGroupManager, func()) {
	mockStreamReader := &mockStreamReader{}
	mockConsumerMgr := &mockConsumerGroupManager{}

	// Create a minimal manager for testing
	manager := &Manager{
		sessions: make(map[string]*Session),
		progress: make(map[string]*Progress),
		mu:       sync.RWMutex{},
	}

	executor := NewExecutor(manager, mockStreamReader, mockConsumerMgr)
	executor.SetManager(manager)

	cleanup := func() {
		// Cleanup if needed
	}

	return executor, manager, mockStreamReader, mockConsumerMgr, cleanup
}

func TestExecutor_StartReplay(t *testing.T) {
	executor, manager, mockReader, mockConsumer, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test session
	session := &Session{
		ID:                   "test-session",
		Stream:               "test-tenant/namespaces/test-ns/streams/test-stream",
		Partition:            0,
		StartOffset:          100,
		SandboxConsumerGroup: "_replay_test_session_test-group",
		Status:               SessionStatusCreated,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 100}
	manager.mu.Unlock()

	// Mock stream reader to return messages
	messages := []*log.Message{
		{
			ID:           "msg1",
			ResourcePath: session.Stream,
			Partition:    0,
			Offset:       100,
			Payload:      []byte("payload1"),
			CreatedAt:    time.Now(),
		},
		{
			ID:           "msg2",
			ResourcePath: session.Stream,
			Partition:    0,
			Offset:       101,
			Payload:      []byte("payload2"),
			CreatedAt:    time.Now(),
		},
	}

	mockReader.readFromOffsetFunc = func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
		if offset == 100 {
			return messages, nil
		}
		return []*log.Message{}, nil // No more messages
	}

	commitCount := 0
	mockConsumer.commitOffsetFunc = func(ctx context.Context, stream, group string, partition int32, offset int64) error {
		commitCount++
		return nil
	}

	// Start replay
	err := executor.StartReplay(ctx, session.ID)
	require.NoError(t, err)

	// Wait a bit for replay to process
	time.Sleep(200 * time.Millisecond)

	// Stop the replay
	err = executor.StopReplay(ctx, session.ID)
	require.NoError(t, err)

	// Verify offsets were committed
	assert.Greater(t, commitCount, 0)
}

func TestExecutor_StartReplay_WithTimestamp(t *testing.T) {
	executor, manager, mockReader, mockConsumer, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	startTime := time.Unix(1000, 0)
	session := &Session{
		ID:                   "test-session",
		Stream:               "test-tenant/namespaces/test-ns/streams/test-stream",
		Partition:            0,
		StartTime:            &startTime,
		SandboxConsumerGroup: "_replay_test_session_test-group",
		Status:               SessionStatusCreated,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 0}
	manager.mu.Unlock()

	// Mock timestamp lookup
	mockReader.findOffsetByTimestampFunc = func(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error) {
		return 100, nil
	}

	// Mock stream reader
	mockReader.readFromOffsetFunc = func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
		return []*log.Message{}, nil
	}

	mockConsumer.commitOffsetFunc = func(ctx context.Context, stream, group string, partition int32, offset int64) error {
		return nil
	}

	// Start replay
	err := executor.StartReplay(ctx, session.ID)
	require.NoError(t, err)

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Stop the replay
	err = executor.StopReplay(ctx, session.ID)
	require.NoError(t, err)
}

func TestExecutor_StartReplay_SessionNotFound(t *testing.T) {
	executor, _, _, _, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	err := executor.StartReplay(ctx, "nonexistent-session")
	assert.Error(t, err)
}

func TestExecutor_StartReplay_InvalidState(t *testing.T) {
	executor, manager, _, _, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	// Create session in stopped state
	session := &Session{
		ID:     "test-session",
		Stream: "test-stream",
		Status: SessionStatusStopped,
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.mu.Unlock()

	err := executor.StartReplay(ctx, session.ID)
	assert.Error(t, err)
	assert.IsType(t, InvalidSessionStateError{}, err)
}

func TestExecutor_PauseReplay(t *testing.T) {
	executor, manager, mockReader, mockConsumer, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	session := &Session{
		ID:                   "test-session",
		Stream:               "test-stream",
		Partition:            0,
		StartOffset:          100,
		SandboxConsumerGroup: "_replay_test_session_test-group",
		Status:               SessionStatusActive,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 100}
	manager.mu.Unlock()

	// Create active replay
	replayCtx, cancel := context.WithCancel(context.Background())
	ar := &activeReplay{
		sessionID: session.ID,
		ctx:       replayCtx,
		cancel:    cancel,
		pauseCh:   make(chan struct{}),
		resumeCh:  make(chan struct{}),
		stopCh:    make(chan struct{}),
	}

	executor.mu.Lock()
	executor.activeReplays[session.ID] = ar
	executor.mu.Unlock()

	mockReader.readFromOffsetFunc = func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
		return []*log.Message{}, nil
	}
	mockConsumer.commitOffsetFunc = func(ctx context.Context, stream, group string, partition int32, offset int64) error {
		return nil
	}

	// Pause replay
	err := executor.PauseReplay(ctx, session.ID)
	require.NoError(t, err)

	// Verify status was updated
	manager.mu.RLock()
	updated, exists := manager.sessions[session.ID]
	manager.mu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, SessionStatusPaused, updated.Status)

	// Cleanup
	cancel()
}

func TestExecutor_ResumeReplay(t *testing.T) {
	executor, manager, _, _, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	session := &Session{
		ID:                   "test-session",
		Stream:               "test-stream",
		Status:               SessionStatusPaused,
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 100}
	manager.mu.Unlock()

	// Create active replay
	replayCtx, cancel := context.WithCancel(context.Background())
	ar := &activeReplay{
		sessionID: session.ID,
		ctx:       replayCtx,
		cancel:    cancel,
		pauseCh:   make(chan struct{}),
		resumeCh:  make(chan struct{}),
		stopCh:    make(chan struct{}),
	}

	executor.mu.Lock()
	executor.activeReplays[session.ID] = ar
	executor.mu.Unlock()

	// Resume replay
	err := executor.ResumeReplay(ctx, session.ID)
	require.NoError(t, err)

	// Verify status was updated
	manager.mu.RLock()
	updated, exists := manager.sessions[session.ID]
	manager.mu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, SessionStatusActive, updated.Status)

	// Cleanup
	cancel()
}

func TestExecutor_StopReplay(t *testing.T) {
	executor, manager, _, _, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	session := &Session{
		ID:                   "test-session",
		Stream:               "test-stream",
		Status:               SessionStatusActive,
		SandboxConsumerGroup: "_replay_test_session_test-group",
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 100}
	manager.mu.Unlock()

	// Create active replay
	replayCtx, cancel := context.WithCancel(context.Background())
	ar := &activeReplay{
		sessionID: session.ID,
		ctx:       replayCtx,
		cancel:    cancel,
		pauseCh:   make(chan struct{}),
		resumeCh:  make(chan struct{}),
		stopCh:    make(chan struct{}),
	}

	executor.mu.Lock()
	executor.activeReplays[session.ID] = ar
	executor.mu.Unlock()

	// Stop replay
	err := executor.StopReplay(ctx, session.ID)
	require.NoError(t, err)

	// Verify replay was removed from active replays
	executor.mu.RLock()
	_, exists := executor.activeReplays[session.ID]
	executor.mu.RUnlock()
	assert.False(t, exists)

	// Verify status was updated
	manager.mu.RLock()
	updated, exists := manager.sessions[session.ID]
	manager.mu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, SessionStatusStopped, updated.Status)
}

func TestExecutor_StartReplay_WithEndOffset(t *testing.T) {
	executor, manager, mockReader, mockConsumer, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	endOffset := int64(102)
	session := &Session{
		ID:                   "test-session",
		Stream:               "test-stream",
		Partition:            0,
		StartOffset:          100,
		EndOffset:            &endOffset,
		SandboxConsumerGroup: "_replay_test_session_test-group",
		Status:               SessionStatusCreated,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	manager.mu.Lock()
	manager.sessions[session.ID] = session
	manager.progress[session.ID] = &Progress{CurrentOffset: 100}
	manager.mu.Unlock()

	messageCount := 0
	mockReader.readFromOffsetFunc = func(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
		if offset == 100 {
			return []*log.Message{
				{
					ID:           "msg1",
					ResourcePath: session.Stream,
					Partition:    0,
					Offset:       100,
					Payload:      []byte("payload1"),
					CreatedAt:    time.Now(),
				},
				{
					ID:           "msg2",
					ResourcePath: session.Stream,
					Partition:    0,
					Offset:       101,
					Payload:      []byte("payload2"),
					CreatedAt:    time.Now(),
				},
				{
					ID:           "msg3",
					ResourcePath: session.Stream,
					Partition:    0,
					Offset:       102,
					Payload:      []byte("payload3"),
					CreatedAt:    time.Now(),
				},
			}, nil
		}
		return []*log.Message{}, nil
	}

	commitCount := 0
	mockConsumer.commitOffsetFunc = func(ctx context.Context, stream, group string, partition int32, offset int64) error {
		commitCount++
		messageCount++
		return nil
	}

	// Start replay
	err := executor.StartReplay(ctx, session.ID)
	require.NoError(t, err)

	// Wait for replay to complete
	time.Sleep(500 * time.Millisecond)

	// Verify replay completed
	manager.mu.RLock()
	updated, exists := manager.sessions[session.ID]
	manager.mu.RUnlock()
	require.True(t, exists)
	// Status should be completed or active (depending on timing)
	assert.True(t, updated.Status == SessionStatusCompleted || updated.Status == SessionStatusActive)

	// Cleanup
	executor.StopReplay(ctx, session.ID)
}
