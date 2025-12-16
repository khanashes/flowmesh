package replay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

const (
	// DefaultReplaySessionsFile is the default filename for replay sessions persistence
	DefaultReplaySessionsFile = "replay_sessions.json"
)

// Manager manages replay sessions
type Manager struct {
	metaStore *metastore.Store
	filePath  string
	sessions  map[string]*Session
	progress  map[string]*Progress
	mu        sync.RWMutex
	log       zerolog.Logger
	ready     bool
	executor  *Executor
}

// NewManager creates a new replay manager
func NewManager(metaStore *metastore.Store, metadataDir string, executor *Executor) *Manager {
	filePath := filepath.Join(metadataDir, DefaultReplaySessionsFile)
	return &Manager{
		metaStore: metaStore,
		filePath:  filePath,
		sessions:  make(map[string]*Session),
		progress:  make(map[string]*Progress),
		log:       logger.WithComponent("replay"),
		executor:  executor,
		ready:     false,
	}
}

// Start initializes and starts the manager
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ready {
		return nil
	}

	// Load existing sessions from disk
	if err := m.load(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to load replay sessions: %w", err)
		}
		m.log.Info().Str("file", m.filePath).Msg("Replay sessions file does not exist, will be created on first session")
	}

	m.ready = true
	m.log.Info().Msg("Replay manager started")
	return nil
}

// Stop gracefully stops the manager
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.ready {
		m.mu.Unlock()
		return nil
	}

	// Collect active session IDs while holding the lock
	activeSessions := make([]string, 0)
	for sessionID, session := range m.sessions {
		if session.Status == SessionStatusActive {
			activeSessions = append(activeSessions, sessionID)
		}
	}
	m.mu.Unlock()

	// Stop all active replays (release lock before calling StopReplay to avoid deadlock)
	// StopReplay may call UpdateSessionStatus which needs the lock
	for _, sessionID := range activeSessions {
		if err := m.executor.StopReplay(ctx, sessionID); err != nil {
			m.log.Error().Err(err).Str("session", sessionID).Msg("Failed to stop replay on shutdown")
		}
	}

	// Re-acquire lock for final cleanup
	m.mu.Lock()
	defer m.mu.Unlock()

	// Flush sessions to disk
	if err := m.flush(); err != nil {
		m.log.Error().Err(err).Msg("Failed to flush replay sessions on stop")
		return fmt.Errorf("failed to flush replay sessions: %w", err)
	}

	m.ready = false
	m.log.Info().Msg("Replay manager stopped")
	return nil
}

// Ready returns true if the manager is ready
func (m *Manager) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// CreateSession creates a new replay session (internal method with StartRequest)
func (m *Manager) CreateSessionWithRequest(ctx context.Context, req StartRequest) (*Session, error) {
	if !m.Ready() {
		return nil, fmt.Errorf("replay manager is not ready")
	}

	// Validate stream exists
	_, err := m.metaStore.GetResource(req.Stream)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, StreamNotFoundError{Stream: req.Stream}
		}
		return nil, err
	}

	// Generate session ID
	sessionID, err := generateSessionID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Determine start offset
	var startOffset int64
	if req.StartOffset != nil {
		startOffset = *req.StartOffset
		if startOffset < 0 {
			return nil, InvalidOffsetError{Offset: startOffset, Reason: "start offset cannot be negative"}
		}
	} else if req.StartTime != nil {
		// Start offset will be determined by executor when replay starts
		// For now, set to 0 as placeholder
		startOffset = 0
	} else {
		return nil, fmt.Errorf("either start_offset or start_time must be provided")
	}

	// Determine end offset (optional)
	var endOffset *int64
	if req.EndOffset != nil {
		endOffset = req.EndOffset
		// Only validate endOffset >= startOffset if both are offsets (not timestamps)
		if req.StartOffset != nil && *endOffset < startOffset {
			return nil, InvalidOffsetError{Offset: *endOffset, Reason: "end offset must be >= start offset"}
		}
	}

	// Validate sandbox consumer group name
	if err := ValidateReplayRequest(req); err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		ID:                   sessionID,
		Stream:               req.Stream,
		Partition:            req.Partition,
		StartOffset:          startOffset,
		StartTime:            req.StartTime,
		EndOffset:            endOffset,
		EndTime:              req.EndTime,
		SandboxConsumerGroup: req.SandboxConsumerGroup,
		Status:               SessionStatusCreated,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	m.mu.Lock()
	m.sessions[sessionID] = session
	m.progress[sessionID] = &Progress{
		CurrentOffset:    startOffset,
		MessagesReplayed: 0,
		Errors:           0,
	}
	m.mu.Unlock()

	// Persist to disk
	if err := m.flush(); err != nil {
		m.mu.Lock()
		delete(m.sessions, sessionID)
		delete(m.progress, sessionID)
		m.mu.Unlock()
		return nil, fmt.Errorf("failed to persist session: %w", err)
	}

	m.log.Info().
		Str("session", sessionID).
		Str("stream", req.Stream).
		Int64("start_offset", startOffset).
		Msg("Replay session created")

	return session, nil
}

// GetSession retrieves a replay session by ID
func (m *Manager) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return nil, SessionNotFoundError{SessionID: sessionID}
	}

	// Return a copy to avoid data races
	sessionCopy := *session
	return &sessionCopy, nil
}

// ListSessions lists all replay sessions, optionally filtered by stream
func (m *Manager) ListSessions(ctx context.Context, stream string) ([]*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sessions := make([]*Session, 0)
	for _, session := range m.sessions {
		if stream == "" || session.Stream == stream {
			// Return a copy
			sessionCopy := *session
			sessions = append(sessions, &sessionCopy)
		}
	}

	return sessions, nil
}

// UpdateSessionStatus updates the status of a replay session
func (m *Manager) UpdateSessionStatus(ctx context.Context, sessionID string, status SessionStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return SessionNotFoundError{SessionID: sessionID}
	}

	// Allow no-op transitions (same state to same state)
	if session.Status == status {
		return nil
	}

	// Validate state transition
	if !isValidStateTransition(session.Status, status) {
		return InvalidSessionStateError{
			SessionID: sessionID,
			Current:   session.Status,
			Requested: status,
		}
	}

	session.Status = status
	session.UpdatedAt = time.Now()

	// Persist to disk
	if err := m.flush(); err != nil {
		return fmt.Errorf("failed to persist session status update: %w", err)
	}

	return nil
}

// DeleteSession deletes a replay session
func (m *Manager) DeleteSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return SessionNotFoundError{SessionID: sessionID}
	}

	// Stop replay if active
	if session.Status == SessionStatusActive {
		if err := m.executor.StopReplay(ctx, sessionID); err != nil {
			m.log.Warn().Err(err).Str("session", sessionID).Msg("Failed to stop replay before deletion")
		}
	}

	delete(m.sessions, sessionID)
	delete(m.progress, sessionID)

	// Persist to disk
	if err := m.flush(); err != nil {
		return fmt.Errorf("failed to persist session deletion: %w", err)
	}

	m.log.Info().Str("session", sessionID).Msg("Replay session deleted")
	return nil
}

// GetReplayProgress retrieves the progress of a replay session
func (m *Manager) GetReplayProgress(ctx context.Context, sessionID string) (*Progress, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	progress, exists := m.progress[sessionID]
	if !exists {
		return nil, SessionNotFoundError{SessionID: sessionID}
	}

	// Return a copy
	progressCopy := *progress
	return &progressCopy, nil
}

// StartReplay starts replaying messages for a session
func (m *Manager) StartReplay(ctx context.Context, sessionID string) error {
	return m.executor.StartReplay(ctx, sessionID)
}

// PauseReplay pauses an active replay session
func (m *Manager) PauseReplay(ctx context.Context, sessionID string) error {
	return m.executor.PauseReplay(ctx, sessionID)
}

// ResumeReplay resumes a paused replay session
func (m *Manager) ResumeReplay(ctx context.Context, sessionID string) error {
	return m.executor.ResumeReplay(ctx, sessionID)
}

// StopReplay stops an active replay session
func (m *Manager) StopReplay(ctx context.Context, sessionID string) error {
	return m.executor.StopReplay(ctx, sessionID)
}

// CreateSession creates a new replay session (public API matching interface)
func (m *Manager) CreateSession(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*Session, error) {
	req := StartRequest{
		Stream:               stream,
		Partition:            0, // Default partition
		SandboxConsumerGroup: sandboxGroup,
	}

	// Set start offset or time
	if startTime != nil {
		req.StartTime = startTime
	} else {
		req.StartOffset = &startOffset
	}

	// Set end offset or time
	if endTime != nil {
		req.EndTime = endTime
	} else if endOffset != nil {
		req.EndOffset = endOffset
	}

	return m.CreateSessionWithRequest(ctx, req)
}

// UpdateProgress updates the progress of a replay session
func (m *Manager) UpdateProgress(ctx context.Context, sessionID string, progress *Progress) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[sessionID]; !exists {
		return SessionNotFoundError{SessionID: sessionID}
	}

	// Store a copy to avoid data races
	progressCopy := *progress
	m.progress[sessionID] = &progressCopy
	return nil
}

// load loads replay sessions from disk
func (m *Manager) load() error {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return err
	}

	var sessionsData struct {
		Sessions []*Session `json:"sessions"`
	}

	if err := json.Unmarshal(data, &sessionsData); err != nil {
		return fmt.Errorf("failed to unmarshal replay sessions: %w", err)
	}

	m.sessions = make(map[string]*Session)
	m.progress = make(map[string]*Progress)

	for _, session := range sessionsData.Sessions {
		m.sessions[session.ID] = session
		m.progress[session.ID] = &Progress{
			CurrentOffset:    session.StartOffset,
			MessagesReplayed: 0,
			Errors:           0,
		}
	}

	return nil
}

// flush persists replay sessions to disk
func (m *Manager) flush() error {
	// Skip flush if filePath is empty (e.g., in tests)
	if m.filePath == "" {
		return nil
	}

	sessionsList := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessionsList = append(sessionsList, session)
	}

	data := struct {
		Sessions []*Session `json:"sessions"`
	}{
		Sessions: sessionsList,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal replay sessions: %w", err)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(m.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create replay sessions directory: %w", err)
	}

	// Write to temp file first, then rename (atomic write)
	tmpFile := m.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, jsonData, 0o600); err != nil {
		return fmt.Errorf("failed to write replay sessions file: %w", err)
	}

	if err := os.Rename(tmpFile, m.filePath); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename replay sessions file: %w", err)
	}

	return nil
}

// generateSessionID generates a unique session ID
func generateSessionID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// isValidStateTransition checks if a state transition is valid
func isValidStateTransition(current, requested SessionStatus) bool {
	switch current {
	case SessionStatusCreated:
		return requested == SessionStatusActive || requested == SessionStatusStopped
	case SessionStatusActive:
		return requested == SessionStatusPaused || requested == SessionStatusStopped || requested == SessionStatusCompleted || requested == SessionStatusError
	case SessionStatusPaused:
		return requested == SessionStatusActive || requested == SessionStatusStopped
	case SessionStatusStopped, SessionStatusCompleted, SessionStatusError:
		return false // Terminal states
	default:
		return false
	}
}
