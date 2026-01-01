package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/flowmesh/engine/internal/storage"
	replayerrors "github.com/flowmesh/engine/internal/storage/replay"
)

// ReplayHandlers provides HTTP handlers for replay operations
type ReplayHandlers struct {
	storage storage.StorageBackend
	wsHub   *Hub // WebSocket hub for real-time updates
}

// NewReplayHandlers creates new replay handlers
func NewReplayHandlers(storage storage.StorageBackend) *ReplayHandlers {
	return &ReplayHandlers{
		storage: storage,
		wsHub:   nil, // Will be set by router if WebSocket is enabled
	}
}

// SetWSHub sets the WebSocket hub
func (h *ReplayHandlers) SetWSHub(hub *Hub) {
	h.wsHub = hub
}

// CreateReplaySessionRequest represents a request to create a replay session
type CreateReplaySessionRequest struct {
	StartOffset          *int64 `json:"start_offset,omitempty"`
	StartTimestamp       *int64 `json:"start_timestamp,omitempty"` // Unix nanoseconds
	EndOffset            *int64 `json:"end_offset,omitempty"`
	EndTimestamp         *int64 `json:"end_timestamp,omitempty"` // Unix nanoseconds
	SandboxConsumerGroup string `json:"sandbox_consumer_group"`
}

// ReplaySessionResponse represents a replay session response
type ReplaySessionResponse struct {
	ID                   string                  `json:"session_id"`
	Stream               string                  `json:"stream"`
	Partition            int32                   `json:"partition"`
	StartOffset          int64                   `json:"start_offset"`
	StartTimestamp       *int64                  `json:"start_timestamp,omitempty"`
	EndOffset            *int64                  `json:"end_offset,omitempty"`
	EndTimestamp         *int64                  `json:"end_timestamp,omitempty"`
	SandboxConsumerGroup string                  `json:"sandbox_consumer_group"`
	Status               string                  `json:"status"`
	Progress             *ReplayProgressResponse `json:"progress,omitempty"`
	CreatedAt            int64                   `json:"created_at"`
	UpdatedAt            int64                   `json:"updated_at"`
}

// ReplayProgressResponse represents replay progress
type ReplayProgressResponse struct {
	CurrentOffset    int64  `json:"current_offset"`
	MessagesReplayed int64  `json:"messages_replayed"`
	Errors           int64  `json:"errors"`
	StartedAt        *int64 `json:"started_at,omitempty"`
	PausedAt         *int64 `json:"paused_at,omitempty"`
	CompletedAt      *int64 `json:"completed_at,omitempty"`
}

// CreateReplaySession handles POST /api/v1/replay/sessions
func (h *ReplayHandlers) CreateReplaySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract stream from query parameter
	stream := r.URL.Query().Get("stream")
	if stream == "" {
		http.Error(w, "stream query parameter is required", http.StatusBadRequest)
		return
	}

	// Convert stream path format (tenant/namespace/name) to resource path format (tenant/namespace/stream/name)
	// The stream parameter comes as "tenant/namespace/name", but MetaStore expects "tenant/namespace/stream/name"
	parts := strings.Split(stream, "/")
	if len(parts) != 3 {
		http.Error(w, "invalid stream format, expected tenant/namespace/name", http.StatusBadRequest)
		return
	}
	resourcePath := fmt.Sprintf("%s/%s/stream/%s", parts[0], parts[1], parts[2])

	// Parse request body
	var req CreateReplaySessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.SandboxConsumerGroup == "" {
		http.Error(w, "sandbox_consumer_group is required", http.StatusBadRequest)
		return
	}

	// Determine start offset or time
	var startOffset int64 = -1
	var startTime *time.Time
	if req.StartTimestamp != nil && *req.StartTimestamp > 0 {
		t := time.Unix(0, *req.StartTimestamp)
		startTime = &t
	} else if req.StartOffset != nil && *req.StartOffset >= 0 {
		startOffset = *req.StartOffset
	} else {
		http.Error(w, "either start_offset or start_timestamp must be provided", http.StatusBadRequest)
		return
	}

	// Determine end offset or time
	var endOffset *int64
	var endTime *time.Time
	if req.EndTimestamp != nil && *req.EndTimestamp > 0 {
		t := time.Unix(0, *req.EndTimestamp)
		endTime = &t
	} else if req.EndOffset != nil && *req.EndOffset >= 0 {
		endOffset = req.EndOffset
	}

	// Create session
	replayMgr := h.storage.ReplayManager()
	session, err := replayMgr.CreateSession(r.Context(), resourcePath, startOffset, startTime, endOffset, endTime, req.SandboxConsumerGroup)
	if err != nil {
		h.writeError(w, err, stream)
		return
	}

	// Get progress (ignore errors as progress is optional)
	//nolint:errcheck // Progress is optional, ignore errors
	progress, _ := replayMgr.GetReplayProgress(r.Context(), session.ID)

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(h.sessionToResponse(session, progress)); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// ListReplaySessions handles GET /api/v1/replay/sessions
func (h *ReplayHandlers) ListReplaySessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stream := r.URL.Query().Get("stream")

	replayMgr := h.storage.ReplayManager()
	sessions, err := replayMgr.ListSessions(r.Context(), stream)
	if err != nil {
		h.writeError(w, err, stream)
		return
	}

	responses := make([]ReplaySessionResponse, len(sessions))
	for i, session := range sessions {
		//nolint:errcheck // Progress is optional, ignore errors
		progress, _ := replayMgr.GetReplayProgress(r.Context(), session.ID)
		responses[i] = h.sessionToResponse(session, progress)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"sessions": responses,
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// GetReplaySession handles GET /api/v1/replay/sessions/{session_id}
func (h *ReplayHandlers) GetReplaySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract session ID from path
	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	session, err := replayMgr.GetSession(r.Context(), sessionID)
	if err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	//nolint:errcheck // Progress is optional, ignore errors
	progress, _ := replayMgr.GetReplayProgress(r.Context(), session.ID)

	response := h.sessionToResponse(session, progress)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session update via WebSocket
	if h.wsHub != nil {
		h.wsHub.BroadcastReplaySession(session.ID, response)
	}
}

// StartReplay handles POST /api/v1/replay/sessions/{session_id}/start
func (h *ReplayHandlers) StartReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	if err := replayMgr.StartReplay(r.Context(), sessionID); err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "replay started successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session update
	if h.wsHub != nil {
		if session, err := replayMgr.GetSession(r.Context(), sessionID); err == nil {
			//nolint:errcheck // Progress is optional, ignore errors
			progress, _ := replayMgr.GetReplayProgress(r.Context(), sessionID)
			response := h.sessionToResponse(session, progress)
			h.wsHub.BroadcastReplaySession(sessionID, response)
		}
	}
}

// PauseReplay handles POST /api/v1/replay/sessions/{session_id}/pause
func (h *ReplayHandlers) PauseReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	if err := replayMgr.PauseReplay(r.Context(), sessionID); err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "replay paused successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session update
	if h.wsHub != nil {
		if session, err := replayMgr.GetSession(r.Context(), sessionID); err == nil {
			//nolint:errcheck // Progress is optional, ignore errors
			progress, _ := replayMgr.GetReplayProgress(r.Context(), sessionID)
			response := h.sessionToResponse(session, progress)
			h.wsHub.BroadcastReplaySession(sessionID, response)
		}
	}
}

// ResumeReplay handles POST /api/v1/replay/sessions/{session_id}/resume
func (h *ReplayHandlers) ResumeReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	if err := replayMgr.ResumeReplay(r.Context(), sessionID); err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "replay resumed successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session update
	if h.wsHub != nil {
		if session, err := replayMgr.GetSession(r.Context(), sessionID); err == nil {
			//nolint:errcheck // Progress is optional, ignore errors
			progress, _ := replayMgr.GetReplayProgress(r.Context(), sessionID)
			response := h.sessionToResponse(session, progress)
			h.wsHub.BroadcastReplaySession(sessionID, response)
		}
	}
}

// StopReplay handles POST /api/v1/replay/sessions/{session_id}/stop
func (h *ReplayHandlers) StopReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	if err := replayMgr.StopReplay(r.Context(), sessionID); err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "replay stopped successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session update
	if h.wsHub != nil {
		if session, err := replayMgr.GetSession(r.Context(), sessionID); err == nil {
			//nolint:errcheck // Progress is optional, ignore errors
			progress, _ := replayMgr.GetReplayProgress(r.Context(), sessionID)
			response := h.sessionToResponse(session, progress)
			h.wsHub.BroadcastReplaySession(sessionID, response)
		}
	}
}

// DeleteReplaySession handles DELETE /api/v1/replay/sessions/{session_id}
func (h *ReplayHandlers) DeleteReplaySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := extractSessionIDFromPath(r.URL.Path)
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	replayMgr := h.storage.ReplayManager()
	if err := replayMgr.DeleteSession(r.Context(), sessionID); err != nil {
		h.writeError(w, err, sessionID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "replay session deleted successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast replay session deletion
	if h.wsHub != nil {
		h.wsHub.BroadcastReplaySession(sessionID, map[string]interface{}{
			"id":     sessionID,
			"status": "deleted",
		})
	}
}

// Helper functions

func (h *ReplayHandlers) sessionToResponse(session *storage.ReplaySession, progress *storage.ReplayProgress) ReplaySessionResponse {
	resp := ReplaySessionResponse{
		ID:                   session.ID,
		Stream:               session.Stream,
		Partition:            session.Partition,
		StartOffset:          session.StartOffset,
		EndOffset:            session.EndOffset,
		SandboxConsumerGroup: session.SandboxConsumerGroup,
		Status:               session.Status,
		CreatedAt:            session.CreatedAt.UnixNano(),
		UpdatedAt:            session.UpdatedAt.UnixNano(),
	}

	if session.StartTime != nil {
		ts := session.StartTime.UnixNano()
		resp.StartTimestamp = &ts
	}
	if session.EndTime != nil {
		ts := session.EndTime.UnixNano()
		resp.EndTimestamp = &ts
	}

	if progress != nil {
		resp.Progress = &ReplayProgressResponse{
			CurrentOffset:    progress.CurrentOffset,
			MessagesReplayed: progress.MessagesReplayed,
			Errors:           progress.Errors,
		}
		if progress.StartedAt != nil {
			ts := progress.StartedAt.UnixNano()
			resp.Progress.StartedAt = &ts
		}
		if progress.PausedAt != nil {
			ts := progress.PausedAt.UnixNano()
			resp.Progress.PausedAt = &ts
		}
		if progress.CompletedAt != nil {
			ts := progress.CompletedAt.UnixNano()
			resp.Progress.CompletedAt = &ts
		}
	}

	return resp
}

func extractSessionIDFromPath(path string) string {
	// Path format: /api/v1/replay/sessions/{session_id} or /api/v1/replay/sessions/{session_id}/start, etc.
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "sessions" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (h *ReplayHandlers) writeError(w http.ResponseWriter, err error, resource string) {
	switch e := err.(type) {
	case replayerrors.SessionNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case replayerrors.InvalidSessionStateError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	case replayerrors.StreamNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case replayerrors.InvalidOffsetError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	case replayerrors.InvalidTimestampError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	default:
		// Check if it's a validation error (contains common validation keywords)
		errMsg := err.Error()
		if strings.Contains(errMsg, "sandbox consumer group") ||
			strings.Contains(errMsg, "is required") ||
			strings.Contains(errMsg, "cannot be") ||
			strings.Contains(errMsg, "must be") ||
			strings.Contains(errMsg, "must start with") {
			http.Error(w, errMsg, http.StatusBadRequest)
		} else {
			http.Error(w, errMsg, http.StatusInternalServerError)
		}
	}
}
