package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/rs/zerolog"
)

// Executor executes replay sessions
type Executor struct {
	manager       *Manager
	streamReader  StreamReader
	consumerMgr   ConsumerGroupManager
	activeReplays map[string]*activeReplay
	mu            sync.RWMutex
	log           zerolog.Logger
}

// StreamReader interface for reading stream messages
type StreamReader interface {
	ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error)
	FindOffsetByTimestamp(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error)
}

// ConsumerGroupManager interface for managing consumer groups
type ConsumerGroupManager interface {
	CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error
}

// NewExecutor creates a new replay executor
func NewExecutor(manager *Manager, streamReader StreamReader, consumerMgr ConsumerGroupManager) *Executor {
	return &Executor{
		manager:       manager,
		streamReader:  streamReader,
		consumerMgr:   consumerMgr,
		activeReplays: make(map[string]*activeReplay),
		log:           logger.WithComponent("replay.executor"),
	}
}

// SetManager sets the manager reference (for circular dependency resolution)
func (e *Executor) SetManager(manager *Manager) {
	e.manager = manager
}

// activeReplay represents an active replay session
type activeReplay struct {
	sessionID string
	ctx       context.Context
	cancel    context.CancelFunc
	pauseCh   chan struct{}
	resumeCh  chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// StartReplay starts replaying messages for a session
func (e *Executor) StartReplay(ctx context.Context, sessionID string) error {
	// Get session
	session, err := e.manager.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}

	// Validate session can be started
	if session.Status != SessionStatusCreated && session.Status != SessionStatusPaused {
		return InvalidSessionStateError{
			SessionID: sessionID,
			Current:   session.Status,
			Requested: SessionStatusActive,
		}
	}

	// Check if already active
	e.mu.Lock()
	if _, exists := e.activeReplays[sessionID]; exists {
		e.mu.Unlock()
		return fmt.Errorf("replay session %s is already active", sessionID)
	}

	// Create replay context
	replayCtx, cancel := context.WithCancel(ctx)
	ar := &activeReplay{
		sessionID: sessionID,
		ctx:       replayCtx,
		cancel:    cancel,
		pauseCh:   make(chan struct{}),
		resumeCh:  make(chan struct{}),
		stopCh:    make(chan struct{}),
	}
	e.activeReplays[sessionID] = ar
	e.mu.Unlock()

	// Update session status
	if err := e.manager.UpdateSessionStatus(ctx, sessionID, SessionStatusActive); err != nil {
		e.mu.Lock()
		delete(e.activeReplays, sessionID)
		e.mu.Unlock()
		cancel()
		return err
	}

	// Update progress (ignore errors as progress is optional)
	//nolint:errcheck // Progress is optional, ignore errors
	progress, _ := e.manager.GetReplayProgress(ctx, sessionID)
	if progress != nil {
		now := time.Now()
		progress.StartedAt = &now
		progress.PausedAt = nil
		if err := e.manager.UpdateProgress(ctx, sessionID, progress); err != nil {
			e.log.Warn().Err(err).Str("session", sessionID).Msg("Failed to update progress on start")
		}
	}

	// Start replay goroutine
	ar.wg.Add(1)
	go e.runReplay(ar, session)

	e.log.Info().
		Str("session", sessionID).
		Str("stream", session.Stream).
		Int64("start_offset", session.StartOffset).
		Msg("Replay started")

	return nil
}

// PauseReplay pauses an active replay session
func (e *Executor) PauseReplay(ctx context.Context, sessionID string) error {
	e.mu.RLock()
	ar, exists := e.activeReplays[sessionID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("replay session %s is not active", sessionID)
	}

	// Update session status
	if err := e.manager.UpdateSessionStatus(ctx, sessionID, SessionStatusPaused); err != nil {
		return err
	}

	// Signal pause
	select {
	case ar.pauseCh <- struct{}{}:
	default:
	}

	// Update progress (ignore errors as progress is optional)
	//nolint:errcheck // Progress is optional, ignore errors
	progress, _ := e.manager.GetReplayProgress(ctx, sessionID)
	if progress != nil {
		now := time.Now()
		progress.PausedAt = &now
		if err := e.manager.UpdateProgress(ctx, sessionID, progress); err != nil {
			e.log.Warn().Err(err).Str("session", sessionID).Msg("Failed to update progress on pause")
		}
	}

	e.log.Info().Str("session", sessionID).Msg("Replay paused")
	return nil
}

// ResumeReplay resumes a paused replay session
func (e *Executor) ResumeReplay(ctx context.Context, sessionID string) error {
	e.mu.RLock()
	ar, exists := e.activeReplays[sessionID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("replay session %s is not active", sessionID)
	}

	// Update session status
	if err := e.manager.UpdateSessionStatus(ctx, sessionID, SessionStatusActive); err != nil {
		return err
	}

	// Signal resume
	select {
	case ar.resumeCh <- struct{}{}:
	default:
	}

	// Update progress (ignore errors as progress is optional)
	//nolint:errcheck // Progress is optional, ignore errors
	progress, _ := e.manager.GetReplayProgress(ctx, sessionID)
	if progress != nil {
		progress.PausedAt = nil
		if err := e.manager.UpdateProgress(ctx, sessionID, progress); err != nil {
			e.log.Warn().Err(err).Str("session", sessionID).Msg("Failed to update progress on resume")
		}
	}

	e.log.Info().Str("session", sessionID).Msg("Replay resumed")
	return nil
}

// StopReplay stops an active replay session
func (e *Executor) StopReplay(ctx context.Context, sessionID string) error {
	e.mu.Lock()
	ar, exists := e.activeReplays[sessionID]
	if exists {
		delete(e.activeReplays, sessionID)
	}
	e.mu.Unlock()

	if !exists {
		// Session might already be stopped, try to update status anyway
		return e.manager.UpdateSessionStatus(ctx, sessionID, SessionStatusStopped)
	}

	// Cancel context
	ar.cancel()

	// Signal stop
	select {
	case ar.stopCh <- struct{}{}:
	default:
	}

	// Wait for goroutine to finish
	ar.wg.Wait()

	// Update session status
	if err := e.manager.UpdateSessionStatus(ctx, sessionID, SessionStatusStopped); err != nil {
		return err
	}

	e.log.Info().Str("session", sessionID).Msg("Replay stopped")
	return nil
}

// runReplay runs the replay loop for a session
func (e *Executor) runReplay(ar *activeReplay, session *Session) {
	defer ar.wg.Done()

	// Determine start offset
	startOffset := session.StartOffset
	if session.StartTime != nil {
		// Lookup offset by timestamp
		offset, err := e.streamReader.FindOffsetByTimestamp(ar.ctx, session.Stream, session.Partition, *session.StartTime)
		if err != nil {
			e.log.Error().Err(err).Str("session", ar.sessionID).Msg("Failed to find offset by timestamp")
			if updateErr := e.manager.UpdateSessionStatus(ar.ctx, ar.sessionID, SessionStatusError); updateErr != nil {
				e.log.Warn().Err(updateErr).Str("session", ar.sessionID).Msg("Failed to update session status to error")
			}
			return
		}
		startOffset = offset
	}

	// Determine end offset
	var endOffset int64 = -1 // No end by default
	if session.EndOffset != nil {
		endOffset = *session.EndOffset
	} else if session.EndTime != nil {
		// Lookup offset by timestamp
		offset, err := e.streamReader.FindOffsetByTimestamp(ar.ctx, session.Stream, session.Partition, *session.EndTime)
		if err != nil {
			e.log.Error().Err(err).Str("session", ar.sessionID).Msg("Failed to find end offset by timestamp")
			if updateErr := e.manager.UpdateSessionStatus(ar.ctx, ar.sessionID, SessionStatusError); updateErr != nil {
				e.log.Warn().Err(updateErr).Str("session", ar.sessionID).Msg("Failed to update session status to error")
			}
			return
		}
		endOffset = offset
	}

	currentOffset := startOffset
	batchSize := 100
	paused := false

	// Get initial progress
	progress, _ := e.manager.GetReplayProgress(ar.ctx, ar.sessionID)
	if progress == nil {
		progress = &Progress{CurrentOffset: startOffset}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ar.ctx.Done():
			return
		case <-ar.stopCh:
			return
		case <-ar.pauseCh:
			paused = true
		case <-ar.resumeCh:
			paused = false
		case <-ticker.C:
			if paused {
				continue
			}

			// Check if we've reached the end
			if endOffset >= 0 && currentOffset > endOffset {
				// Replay completed
				if err := e.manager.UpdateSessionStatus(ar.ctx, ar.sessionID, SessionStatusCompleted); err != nil {
					e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update session status to completed")
				}
				now := time.Now()
				progress.CompletedAt = &now
				if err := e.manager.UpdateProgress(ar.ctx, ar.sessionID, progress); err != nil {
					e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update progress")
				}
				e.mu.Lock()
				delete(e.activeReplays, ar.sessionID)
				e.mu.Unlock()
				return
			}

			// Read messages
			messages, err := e.streamReader.ReadFromOffset(ar.ctx, session.Stream, session.Partition, currentOffset, batchSize)
			if err != nil {
				e.log.Error().Err(err).Str("session", ar.sessionID).Int64("offset", currentOffset).Msg("Failed to read messages")
				progress.Errors++
				if updateErr := e.manager.UpdateProgress(ar.ctx, ar.sessionID, progress); updateErr != nil {
					e.log.Warn().Err(updateErr).Str("session", ar.sessionID).Msg("Failed to update progress after error")
				}
				continue
			}

			if len(messages) == 0 {
				// No more messages available, but we haven't reached end offset
				// This might mean we're waiting for more messages or reached the end
				if endOffset >= 0 && currentOffset >= endOffset {
					// Replay completed
					if err := e.manager.UpdateSessionStatus(ar.ctx, ar.sessionID, SessionStatusCompleted); err != nil {
						e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update session status to completed")
					}
					now := time.Now()
					progress.CompletedAt = &now
					if err := e.manager.UpdateProgress(ar.ctx, ar.sessionID, progress); err != nil {
						e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update progress")
					}
					e.mu.Lock()
					delete(e.activeReplays, ar.sessionID)
					e.mu.Unlock()
					return
				}
				continue
			}

			// Process messages
			for _, msg := range messages {
				// Check if we've reached the end
				if endOffset >= 0 && msg.Offset > endOffset {
					// Replay completed
					if err := e.manager.UpdateSessionStatus(ar.ctx, ar.sessionID, SessionStatusCompleted); err != nil {
						e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update session status to completed")
					}
					now := time.Now()
					progress.CompletedAt = &now
					if err := e.manager.UpdateProgress(ar.ctx, ar.sessionID, progress); err != nil {
						e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update progress")
					}
					e.mu.Lock()
					delete(e.activeReplays, ar.sessionID)
					e.mu.Unlock()
					return
				}

				// Commit offset to sandbox consumer group
				if err := e.consumerMgr.CommitOffset(ar.ctx, session.Stream, session.SandboxConsumerGroup, session.Partition, msg.Offset); err != nil {
					e.log.Error().Err(err).
						Str("session", ar.sessionID).
						Str("group", session.SandboxConsumerGroup).
						Int64("offset", msg.Offset).
						Msg("Failed to commit offset")
					progress.Errors++
				} else {
					progress.MessagesReplayed++
				}

				currentOffset = msg.Offset + 1
				progress.CurrentOffset = currentOffset
			}

			// Update progress periodically
			if err := e.manager.UpdateProgress(ar.ctx, ar.sessionID, progress); err != nil {
				e.log.Warn().Err(err).Str("session", ar.sessionID).Msg("Failed to update progress")
			}
		}
	}
}
