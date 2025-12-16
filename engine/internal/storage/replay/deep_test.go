package replay

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// InMemoryStream simulates a log-based stream
type InMemoryStream struct {
	messages []*log.Message
	mu       sync.RWMutex
}

func NewInMemoryStream(count int) *InMemoryStream {
	s := &InMemoryStream{
		messages: make([]*log.Message, 0, count),
	}
	baseTime := time.Now().Add(-1 * time.Hour)
	for i := 0; i < count; i++ {
		s.messages = append(s.messages, &log.Message{
			ID:           fmt.Sprintf("msg-%d", i),
			ResourcePath: "test-stream",
			Partition:    0,
			Offset:       int64(i),
			Payload:      []byte(fmt.Sprintf("payload-%d", i)),
			CreatedAt:    baseTime.Add(time.Duration(i) * time.Second),
		})
	}
	return s
}

func (s *InMemoryStream) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < 0 || offset >= int64(len(s.messages)) {
		return []*log.Message{}, nil
	}

	end := int(offset) + maxMessages
	if end > len(s.messages) {
		end = len(s.messages)
	}

	// copy messages to check for race conditions later
	msgs := make([]*log.Message, end-int(offset))
	copy(msgs, s.messages[int(offset):end])
	return msgs, nil
}

func (s *InMemoryStream) FindOffsetByTimestamp(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Simple binary search or linear scan since it is in-memory
	for _, msg := range s.messages {
		if !msg.CreatedAt.Before(timestamp) {
			return msg.Offset, nil
		}
	}
	return int64(len(s.messages)), nil
}

// MemoryConsumerGroupMgr tracks commits in memory
type MemoryConsumerGroupMgr struct {
	commits map[string]int64 // group -> lastOffset
	mu      sync.Mutex
}

func NewMemoryConsumerGroupMgr() *MemoryConsumerGroupMgr {
	return &MemoryConsumerGroupMgr{
		commits: make(map[string]int64),
	}
}

func (m *MemoryConsumerGroupMgr) CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits[group] = offset
	return nil
}

func (m *MemoryConsumerGroupMgr) GetCommit(group string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits[group]
}

func TestReplay_DeepIntegration(t *testing.T) {
	// 1. Setup Environment
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create dependencies
	streamSize := 1000
	stream := NewInMemoryStream(streamSize)
	consumerMgr := NewMemoryConsumerGroupMgr()

	// Create temporary directory for replay manager persistence
	tmpDir := t.TempDir()

	// Create metadata store (mocking mostly unused)
	metaStore := &metastore.Store{} // Mock if needed, but only GetResource used

	manager := NewManager(metaStore, tmpDir, nil)
	executor := NewExecutor(manager, stream, consumerMgr)
	manager.executor = executor // wire up circular dep

	// Hack: mock metastore validation in CreateSessionWithRequest
	// Since we can't easily mock the un-exported MetaStore field's behavior without a real one or interface,
	// let's just make sure the manager.metaStore check passes or mock it if it's an interface.
	// Oh, metastore.Store is a struct. We might need to construct a real one or assume checking is skipped if not set?
	// Looking at manager.go: _, err := m.metaStore.GetResource(req.Stream)
	// That will panic if metaStore is nil or fail if empty.
	// Let's rely on the fact that we can just bypass CreateSessionWithRequest and create the session struct directly for testing execution
	// OR better: Create a minimal mock for metastore if possible. But metastore.Store is struct.
	// We'll construct the session manually to bypass validation for this deep test.

	err := manager.Start(ctx)
	require.NoError(t, err)
	defer manager.Stop(ctx)

	// 2. Define Test Scenarios
	t.Run("FullReplay_Lifecycle", func(t *testing.T) {
		startOffset := int64(100)
		endOffset := int64(900)
		sessionID := "deep-test-session-1"

		session := &Session{
			ID:                   sessionID,
			Stream:               "test-stream",
			Partition:            0,
			StartOffset:          startOffset,
			EndOffset:            &endOffset,
			SandboxConsumerGroup: "replay-group-1",
			Status:               SessionStatusCreated,
			CreatedAt:            time.Now(),
		}

		// Manually inject session to bypass metastore check
		manager.mu.Lock()
		manager.sessions[sessionID] = session
		manager.progress[sessionID] = &Progress{CurrentOffset: startOffset}
		manager.mu.Unlock()

		// Start Replay
		err := manager.StartReplay(ctx, sessionID)
		require.NoError(t, err)

		// Wait for some progress
		require.Eventually(t, func() bool {
			p, _ := manager.GetReplayProgress(ctx, sessionID)
			return p.CurrentOffset > startOffset+100
		}, 2*time.Second, 50*time.Millisecond, "Replay should progress")

		// Pause Replay
		err = manager.PauseReplay(ctx, sessionID)
		require.NoError(t, err)

		// Verify status
		sess, err := manager.GetSession(ctx, sessionID)
		require.NoError(t, err)
		require.Equal(t, SessionStatusPaused, sess.Status)

		// Record progress
		progBeforeResume, _ := manager.GetReplayProgress(ctx, sessionID)

		// Sleep a bit to ensure no more progress
		time.Sleep(100 * time.Millisecond)
		progAfterSleep, _ := manager.GetReplayProgress(ctx, sessionID)
		assert.Equal(t, progBeforeResume.CurrentOffset, progAfterSleep.CurrentOffset, "Should not progress while paused")

		// Resume Replay
		err = manager.ResumeReplay(ctx, sessionID)
		require.NoError(t, err)

		// Wait for completion
		require.Eventually(t, func() bool {
			sess, _ := manager.GetSession(ctx, sessionID)
			return sess.Status == SessionStatusCompleted
		}, 5*time.Second, 100*time.Millisecond, "Replay should complete")

		// Verify final state
		progFinal, _ := manager.GetReplayProgress(ctx, sessionID)
		// Expected: it should have consumed up to endOffset (inclusive or exclusive depending on impl)
		// Default implementation ReadStream usually reads up to offset.
		// Let's check consumer commit.
		lastCommit := consumerMgr.GetCommit("replay-group-1")
		assert.GreaterOrEqual(t, lastCommit, endOffset, "Should have committed up to end offset")
		assert.Equal(t, int64(801), progFinal.MessagesReplayed, "Should replay exact number of messages (900-100+1)")
	})

	t.Run("TimeBased_Replay", func(t *testing.T) {
		// Test lookup by timestamp
		startTime := stream.messages[200].CreatedAt
		endTime := stream.messages[300].CreatedAt
		sessionID := "deep-test-session-2"

		session := &Session{
			ID:                   sessionID,
			Stream:               "test-stream",
			Partition:            0,
			StartTime:            &startTime,
			EndTime:              &endTime,
			SandboxConsumerGroup: "replay-group-2",
			Status:               SessionStatusCreated,
			CreatedAt:            time.Now(),
		}

		manager.mu.Lock()
		manager.sessions[sessionID] = session
		manager.progress[sessionID] = &Progress{CurrentOffset: 0} // Placeholder
		manager.mu.Unlock()

		err := manager.StartReplay(ctx, sessionID)
		require.NoError(t, err)

		// Wait for completion
		require.Eventually(t, func() bool {
			sess, _ := manager.GetSession(ctx, sessionID)
			return sess.Status == SessionStatusCompleted
		}, 5*time.Second, 100*time.Millisecond)

		// Verify roughly correct range was replayed
		lastCommit := consumerMgr.GetCommit("replay-group-2")
		// The lookups should find offset 200 and 300
		assert.GreaterOrEqual(t, lastCommit, int64(300))

		prog, _ := manager.GetReplayProgress(ctx, sessionID)
		assert.Equal(t, int64(101), prog.MessagesReplayed, "Should replay (300-200+1) messages")
	})

	t.Run("Stop_Replay", func(t *testing.T) {
		sessionID := "deep-test-session-stop"
		startOffset := int64(500)

		session := &Session{
			ID:                   sessionID,
			Stream:               "test-stream",
			Partition:            0,
			StartOffset:          startOffset,
			SandboxConsumerGroup: "replay-group-stop",
			Status:               SessionStatusCreated,
			CreatedAt:            time.Now(),
		}

		manager.mu.Lock()
		manager.sessions[sessionID] = session
		manager.progress[sessionID] = &Progress{CurrentOffset: startOffset}
		manager.mu.Unlock()

		err := manager.StartReplay(ctx, sessionID)
		require.NoError(t, err)

		// Let it run slightly
		time.Sleep(50 * time.Millisecond)

		err = manager.StopReplay(ctx, sessionID)
		require.NoError(t, err)

		sess, err := manager.GetSession(ctx, sessionID)
		require.NoError(t, err)
		assert.Equal(t, SessionStatusStopped, sess.Status)

		// Ensure no more commits happen
		lastCommit1 := consumerMgr.GetCommit("replay-group-stop")
		time.Sleep(100 * time.Millisecond)
		lastCommit2 := consumerMgr.GetCommit("replay-group-stop")
		assert.Equal(t, lastCommit1, lastCommit2, "Should not commit after stop")
	})
}
