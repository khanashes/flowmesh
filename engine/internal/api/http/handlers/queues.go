package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	queueerrors "github.com/flowmesh/engine/internal/storage/queues"
)

// QueueHandlers provides HTTP handlers for queue operations
type QueueHandlers struct {
	storage storage.StorageBackend
	wsHub   *Hub // WebSocket hub for real-time updates
}

// NewQueueHandlers creates new queue handlers
func NewQueueHandlers(storage storage.StorageBackend) *QueueHandlers {
	return &QueueHandlers{
		storage: storage,
		wsHub:   nil, // Will be set by router if WebSocket is enabled
	}
}

// SetWSHub sets the WebSocket hub
func (h *QueueHandlers) SetWSHub(hub *Hub) {
	h.wsHub = hub
}

// EnqueueRequest represents a request to enqueue a job
type EnqueueRequest struct {
	Payload      []byte            `json:"payload"`
	DelaySeconds int64             `json:"delay_seconds,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// EnqueueResponse represents a response to enqueueing a job
type EnqueueResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	JobID   string `json:"job_id"`
	Seq     int64  `json:"seq"`
}

// ReserveRequest represents a request to reserve a job
type ReserveRequest struct {
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds,omitempty"`
	LongPollTimeoutSeconds   int64 `json:"long_poll_timeout_seconds,omitempty"`
}

// ReserveResponse represents a response to reserving a job
type ReserveResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Job     *Job   `json:"job,omitempty"`
}

// ReceiveRequest represents a request to receive jobs
type ReceiveRequest struct {
	MaxJobs                  int32 `json:"max_jobs,omitempty"`
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds,omitempty"`
	LongPollTimeoutSeconds   int64 `json:"long_poll_timeout_seconds,omitempty"`
}

// ReceiveResponse represents a response to receiving jobs
type ReceiveResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Jobs    []*Job `json:"jobs"`
}

// ACKRequest represents a request to acknowledge a job
type ACKRequest struct {
	JobID string `json:"job_id"`
}

// ACKResponse represents a response to acknowledging a job
type ACKResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// NACKRequest represents a request to NACK a job
type NACKRequest struct {
	JobID        string `json:"job_id"`
	DelaySeconds int64  `json:"delay_seconds,omitempty"`
}

// NACKResponse represents a response to NACKing a job
type NACKResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// QueueStatsResponse represents queue statistics
type QueueStatsResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Stats   *QueueStats `json:"stats"`
}

// QueueStats represents queue statistics
type QueueStats struct {
	TotalJobs           int64 `json:"total_jobs"`
	PendingJobs         int64 `json:"pending_jobs"`
	InFlightJobs        int64 `json:"in_flight_jobs"`
	CompletedJobs       int64 `json:"completed_jobs"`
	FailedJobs          int64 `json:"failed_jobs"`
	OldestJobAgeSeconds int64 `json:"oldest_job_age_seconds"`
}

// Job represents a job
type Job struct {
	ID           string            `json:"id"`
	ResourcePath string            `json:"resource_path"`
	Seq          int64             `json:"seq"`
	Payload      []byte            `json:"payload"`
	Headers      map[string]string `json:"headers,omitempty"`
	CreatedAt    int64             `json:"created_at"`
	VisibleAt    int64             `json:"visible_at"`
	ReserveUntil int64             `json:"reserve_until,omitempty"`
	Attempts     int32             `json:"attempts"`
}

// WriteEvents handles POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs
func (h *QueueHandlers) Enqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Payload) == 0 {
		http.Error(w, "payload cannot be empty", http.StatusBadRequest)
		return
	}

	// Convert delay
	delay := time.Duration(req.DelaySeconds) * time.Second
	if delay < 0 {
		http.Error(w, "delay_seconds cannot be negative", http.StatusBadRequest)
		return
	}

	// Enqueue job
	queueMgr := h.storage.QueueManager()
	jobID, seq, err := queueMgr.Enqueue(r.Context(), resourcePath, req.Payload, storage.QueueEnqueueOptions{
		Delay:   delay,
		Headers: req.Headers,
	})
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(EnqueueResponse{
		Status:  "success",
		Message: "job enqueued successfully",
		JobID:   jobID,
		Seq:     seq,
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast queue stats update after enqueueing
	if h.wsHub != nil {
		// Get updated stats to broadcast
		if queueStats, err := queueMgr.GetQueueStats(r.Context(), resourcePath); err == nil {
			h.wsHub.BroadcastQueueStats(tenant, namespace, name, &QueueStats{
				TotalJobs:           queueStats.TotalJobs,
				PendingJobs:         queueStats.PendingJobs,
				InFlightJobs:        queueStats.InFlightJobs,
				CompletedJobs:       queueStats.CompletedJobs,
				FailedJobs:          queueStats.FailedJobs,
				OldestJobAgeSeconds: int64(queueStats.OldestJobAge.Seconds()),
			})
		}
	}
}

// Reserve handles POST /api/v1/queues/{tenant}/{namespace}/{name}/reserve
func (h *QueueHandlers) Reserve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body (optional)
	var req ReserveRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Parse query parameters (override body if present)
	if visibilityTimeoutStr := r.URL.Query().Get("visibility_timeout"); visibilityTimeoutStr != "" {
		if val, err := strconv.ParseInt(visibilityTimeoutStr, 10, 64); err == nil {
			req.VisibilityTimeoutSeconds = val
		}
	}
	if longPollStr := r.URL.Query().Get("long_poll_timeout"); longPollStr != "" {
		if val, err := strconv.ParseInt(longPollStr, 10, 64); err == nil {
			req.LongPollTimeoutSeconds = val
		}
	}

	// Convert visibility timeout (default 30 seconds)
	visibilityTimeout := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	if visibilityTimeout > 12*time.Hour {
		http.Error(w, "visibility_timeout_seconds cannot exceed 43200 (12 hours)", http.StatusBadRequest)
		return
	}

	// Reserve job
	queueMgr := h.storage.QueueManager()
	job, err := queueMgr.Reserve(r.Context(), resourcePath, visibilityTimeout)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// If no job available, return empty response
	if job == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(ReserveResponse{
			Status:  "success",
			Message: "no jobs available",
		}); err != nil {
			// Failed to encode response, but we've already written the status code
			return
		}
		return
	}

	// Get job payload and headers
	payload, headers, err := h.getJobPayloadAndHeaders(r.Context(), queueMgr, resourcePath, job.ID)
	if err != nil {
		// Log warning but continue
		payload = []byte{}
		headers = make(map[string]string)
	}

	// Convert to response job
	protoJob := &Job{
		ID:           job.ID,
		ResourcePath: resourcePath,
		Seq:          job.Seq,
		Payload:      payload,
		Headers:      headers,
		CreatedAt:    job.CreatedAt.UnixNano(),
		VisibleAt:    job.VisibleAt.UnixNano(),
		Attempts:     job.Attempts,
	}

	if !job.ReserveUntil.IsZero() {
		protoJob.ReserveUntil = job.ReserveUntil.UnixNano()
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(ReserveResponse{
		Status:  "success",
		Message: "job reserved successfully",
		Job:     protoJob,
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// Receive handles POST /api/v1/queues/{tenant}/{namespace}/{name}/receive
func (h *QueueHandlers) Receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body (optional)
	var req ReceiveRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Parse query parameters (override body if present)
	if maxJobsStr := r.URL.Query().Get("max_jobs"); maxJobsStr != "" {
		if val, err := strconv.ParseInt(maxJobsStr, 10, 32); err == nil {
			req.MaxJobs = int32(val)
		}
	}
	if visibilityTimeoutStr := r.URL.Query().Get("visibility_timeout"); visibilityTimeoutStr != "" {
		if val, err := strconv.ParseInt(visibilityTimeoutStr, 10, 64); err == nil {
			req.VisibilityTimeoutSeconds = val
		}
	}
	if longPollStr := r.URL.Query().Get("long_poll_timeout"); longPollStr != "" {
		if val, err := strconv.ParseInt(longPollStr, 10, 64); err == nil {
			req.LongPollTimeoutSeconds = val
		}
	}

	// Validate max jobs
	maxJobs := int(req.MaxJobs)
	if maxJobs <= 0 {
		maxJobs = 1
	}
	if maxJobs > 100 {
		maxJobs = 100
	}

	// Convert visibility timeout (default 30 seconds)
	visibilityTimeout := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	if visibilityTimeout > 12*time.Hour {
		http.Error(w, "visibility_timeout_seconds cannot exceed 43200 (12 hours)", http.StatusBadRequest)
		return
	}

	// Convert long poll timeout
	longPollTimeout := time.Duration(req.LongPollTimeoutSeconds) * time.Second

	// Receive jobs
	queueMgr := h.storage.QueueManager()
	queueJobs, err := queueMgr.Receive(r.Context(), resourcePath, maxJobs, storage.QueueReserveOptions{
		VisibilityTimeout: visibilityTimeout,
		LongPollTimeout:   longPollTimeout,
		MaxWaitTime:       20 * time.Second,
	})
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Convert to response jobs
	jobs := make([]*Job, 0, len(queueJobs))
	for _, job := range queueJobs {
		// Get job payload and headers
		payload, headers, err := h.getJobPayloadAndHeaders(r.Context(), queueMgr, resourcePath, job.ID)
		if err != nil {
			payload = []byte{}
			headers = make(map[string]string)
		}

		protoJob := &Job{
			ID:           job.ID,
			ResourcePath: resourcePath,
			Seq:          job.Seq,
			Payload:      payload,
			Headers:      headers,
			CreatedAt:    job.CreatedAt.UnixNano(),
			VisibleAt:    job.VisibleAt.UnixNano(),
			Attempts:     job.Attempts,
		}

		if !job.ReserveUntil.IsZero() {
			protoJob.ReserveUntil = job.ReserveUntil.UnixNano()
		}

		jobs = append(jobs, protoJob)
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ReceiveResponse{
		Status:  "success",
		Message: "jobs received successfully",
		Jobs:    jobs,
	})
}

// CommitOffset handles POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/ack
func (h *QueueHandlers) ACK(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, jobID, err := extractQueueJobPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ACK job
	queueMgr := h.storage.QueueManager()
	err = queueMgr.RemoveFromInFlight(r.Context(), resourcePath, jobID)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ACKResponse{
		Status:  "success",
		Message: "job acknowledged successfully",
	})

	// Broadcast queue stats update after ACK
	if h.wsHub != nil {
		if queueStats, err := queueMgr.GetQueueStats(r.Context(), resourcePath); err == nil {
			h.wsHub.BroadcastQueueStats(tenant, namespace, name, &QueueStats{
				TotalJobs:           queueStats.TotalJobs,
				PendingJobs:         queueStats.PendingJobs,
				InFlightJobs:        queueStats.InFlightJobs,
				CompletedJobs:       queueStats.CompletedJobs,
				FailedJobs:          queueStats.FailedJobs,
				OldestJobAgeSeconds: int64(queueStats.OldestJobAge.Seconds()),
			})
		}
	}
}

// NACK handles POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/nack
func (h *QueueHandlers) NACK(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, jobID, err := extractQueueJobPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body (optional, for delay)
	var req NACKRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Convert delay (0 means use backoff)
	var delay time.Duration
	if req.DelaySeconds > 0 {
		delay = time.Duration(req.DelaySeconds) * time.Second
	}

	// NACK job
	queueMgr := h.storage.QueueManager()
	if delay > 0 {
		err = queueMgr.NACKWithDelay(r.Context(), resourcePath, jobID, delay)
	} else {
		err = queueMgr.NACK(r.Context(), resourcePath, jobID)
	}
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(NACKResponse{
		Status:  "success",
		Message: "job NACKed successfully",
	})

	// Broadcast queue stats update after NACK
	if h.wsHub != nil {
		if queueStats, err := queueMgr.GetQueueStats(r.Context(), resourcePath); err == nil {
			h.wsHub.BroadcastQueueStats(tenant, namespace, name, &QueueStats{
				TotalJobs:           queueStats.TotalJobs,
				PendingJobs:         queueStats.PendingJobs,
				InFlightJobs:        queueStats.InFlightJobs,
				CompletedJobs:       queueStats.CompletedJobs,
				FailedJobs:          queueStats.FailedJobs,
				OldestJobAgeSeconds: int64(queueStats.OldestJobAge.Seconds()),
			})
		}
	}
}

// GetQueueStats handles GET /api/v1/queues/{tenant}/{namespace}/{name}/stats
func (h *QueueHandlers) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get queue stats
	queueMgr := h.storage.QueueManager()
	stats, err := queueMgr.GetQueueStats(r.Context(), resourcePath)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Prepare stats response
	statsResponse := &QueueStats{
		TotalJobs:           stats.TotalJobs,
		PendingJobs:         stats.PendingJobs,
		InFlightJobs:        stats.InFlightJobs,
		CompletedJobs:       stats.CompletedJobs,
		FailedJobs:          stats.FailedJobs,
		OldestJobAgeSeconds: int64(stats.OldestJobAge.Seconds()),
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(QueueStatsResponse{
		Status:  "success",
		Message: "queue statistics retrieved successfully",
		Stats:   statsResponse,
	})

	// Broadcast stats update via WebSocket
	if h.wsHub != nil {
		h.wsHub.BroadcastQueueStats(tenant, namespace, name, statsResponse)
	}
}

// ListQueues handles GET /api/v1/queues
func (h *QueueHandlers) ListQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get optional query parameters
	tenant := r.URL.Query().Get("tenant")
	namespace := r.URL.Query().Get("namespace")

	// Get meta store
	metaStore := h.storage.MetaStore()
	if metaStore == nil {
		h.writeError(w, errors.New("meta store not available"), "")
		return
	}

	// List queue resources
	queues, err := metaStore.ListResources(tenant, namespace, metastore.ResourceQueue)
	if err != nil {
		h.writeError(w, err, "")
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ListQueuesResponse{
		Status:  "success",
		Message: "queues retrieved successfully",
		Queues:  queues,
	})
}

// Helper methods

// extractQueuePathParams extracts tenant, namespace, and name from URL path
func extractQueuePathParams(r *http.Request) (tenant, namespace, name string, err error) {
	// Path format: /api/v1/queues/{tenant}/{namespace}/{name}/...
	parts := splitPath(r.URL.Path)
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "queues" {
		return "", "", "", http.ErrMissingFile
	}
	return parts[3], parts[4], parts[5], nil
}

// extractQueueJobPathParams extracts tenant, namespace, name, and job_id from URL path
func extractQueueJobPathParams(r *http.Request) (tenant, namespace, name, jobID string, err error) {
	// Path format: /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/ack or /nack
	parts := splitPath(r.URL.Path)
	if len(parts) < 8 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "queues" || parts[6] != "jobs" {
		return "", "", "", "", http.ErrMissingFile
	}
	return parts[3], parts[4], parts[5], parts[7], nil
}

// splitPath splits a URL path into components
func splitPath(path string) []string {
	var parts []string
	var current string
	for _, char := range path {
		if char == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// getJobPayloadAndHeaders retrieves both payload and headers for a job
func (h *QueueHandlers) getJobPayloadAndHeaders(ctx context.Context, queueMgr storage.QueueManager, resourcePath, jobID string) ([]byte, map[string]string, error) {
	// Get job metadata to find the log position
	job, err := queueMgr.GetInFlight(ctx, resourcePath, jobID)
	if err != nil {
		// Try to get payload only
		payload, err := queueMgr.GetJobPayload(ctx, resourcePath, jobID)
		if err != nil {
			return nil, nil, err
		}
		return payload, make(map[string]string), nil
	}

	// Read full message from log to get headers
	payload, headers, err := h.readJobMessageFromLog(ctx, job.PayloadPos.File, job.PayloadPos.Offset, jobID)
	if err != nil {
		// Fallback to GetJobPayload
		payload, err = queueMgr.GetJobPayload(ctx, resourcePath, jobID)
		if err != nil {
			return nil, nil, err
		}
		return payload, make(map[string]string), nil
	}

	return payload, headers, nil
}

// readJobMessageFromLog reads the full job message from log segment
func (h *QueueHandlers) readJobMessageFromLog(ctx context.Context, filePath string, offset int64, jobID string) ([]byte, map[string]string, error) {
	reader, err := log.NewSegmentReader(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer reader.Close()

	// Read entries until we find the one at the correct offset
	for {
		data, entryOffset, readErr := reader.ReadEntry()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, nil, fmt.Errorf("failed to read entry: %w", readErr)
		}

		// Check if this is the entry we're looking for
		if entryOffset == offset {
			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				return nil, nil, fmt.Errorf("failed to decode message: %w", decodeErr)
			}

			// Verify job ID matches
			if msg.ID != jobID {
				return nil, nil, fmt.Errorf("job ID mismatch: expected %s, got %s", jobID, msg.ID)
			}

			return msg.Payload, msg.Headers, nil
		}

		// If we've passed the offset, the entry wasn't found
		if entryOffset > offset {
			return nil, nil, fmt.Errorf("job entry not found at offset %d", offset)
		}
	}

	return nil, nil, fmt.Errorf("job entry not found")
}

// writeError writes an error response
func (h *QueueHandlers) writeError(w http.ResponseWriter, err error, resourcePath string) {
	switch e := err.(type) {
	case queueerrors.QueueNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case queueerrors.JobNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case queueerrors.InvalidVisibilityTimeoutError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	case queueerrors.EnqueueError:
		http.Error(w, e.Error(), http.StatusInternalServerError)
	case queueerrors.NACKError:
		http.Error(w, e.Error(), http.StatusInternalServerError)
	case queueerrors.MaxAttemptsExceededError:
		http.Error(w, e.Error(), http.StatusPreconditionFailed)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// SetRetryPolicyRequest represents a request to set retry policy
type SetRetryPolicyRequest struct {
	MaxAttempts           int32   `json:"max_attempts"`
	InitialBackoffSeconds int64   `json:"initial_backoff_seconds"`
	MaxBackoffSeconds     int64   `json:"max_backoff_seconds"`
	BackoffMultiplier     float64 `json:"backoff_multiplier"`
	BackoffStrategy       string  `json:"backoff_strategy"`
}

// SetRetryPolicyResponse represents a response to setting retry policy
type SetRetryPolicyResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// GetRetryPolicyResponse represents a response to getting retry policy
type GetRetryPolicyResponse struct {
	Status  string                 `json:"status"`
	Message string                 `json:"message,omitempty"`
	Policy  *SetRetryPolicyRequest `json:"policy,omitempty"`
}

// ListDLQJobsResponse represents a response to listing DLQ jobs
type ListDLQJobsResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Jobs    []*Job `json:"jobs"`
}

// ReplayDLQJobResponse represents a response to replaying a DLQ job
type ReplayDLQJobResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	JobID   string `json:"job_id"`
	Seq     int64  `json:"seq"`
}

// ListQueuesResponse represents a response to listing queues
type ListQueuesResponse struct {
	Status  string                      `json:"status"`
	Message string                      `json:"message,omitempty"`
	Queues  []*metastore.ResourceConfig `json:"queues"`
}

// SetRetryPolicy handles PUT /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
func (h *QueueHandlers) SetRetryPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req SetRetryPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Convert to proto RetryPolicy for validation
	protoPolicy := &flowmeshpb.RetryPolicy{
		MaxAttempts:           req.MaxAttempts,
		InitialBackoffSeconds: req.InitialBackoffSeconds,
		MaxBackoffSeconds:     req.MaxBackoffSeconds,
		BackoffMultiplier:     req.BackoffMultiplier,
		BackoffStrategy:       req.BackoffStrategy,
	}

	// Validate retry policy
	if err := validation.ValidateRetryPolicy(protoPolicy); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert to storage RetryPolicy
	queueMgr := h.storage.QueueManager()
	policy := queueerrors.RetryPolicy{
		MaxAttempts:       req.MaxAttempts,
		InitialBackoff:    time.Duration(req.InitialBackoffSeconds) * time.Second,
		MaxBackoff:        time.Duration(req.MaxBackoffSeconds) * time.Second,
		BackoffMultiplier: req.BackoffMultiplier,
		BackoffStrategy:   queueerrors.BackoffStrategy(req.BackoffStrategy),
	}

	// Set retry policy
	ctx := r.Context()
	if err := queueMgr.SetRetryPolicy(ctx, resourcePath, policy); err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(SetRetryPolicyResponse{
		Status:  "ok",
		Message: "retry policy set successfully",
	})
}

// GetRetryPolicy handles GET /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
func (h *QueueHandlers) GetRetryPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get retry policy
	ctx := r.Context()
	queueMgr := h.storage.QueueManager()
	policy, err := queueMgr.GetRetryPolicy(ctx, resourcePath)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Convert to response format
	resp := GetRetryPolicyResponse{
		Status: "ok",
		Policy: &SetRetryPolicyRequest{
			MaxAttempts:           policy.MaxAttempts,
			InitialBackoffSeconds: int64(policy.InitialBackoff.Seconds()),
			MaxBackoffSeconds:     int64(policy.MaxBackoff.Seconds()),
			BackoffMultiplier:     policy.BackoffMultiplier,
			BackoffStrategy:       string(policy.BackoffStrategy),
		},
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// ListDLQJobs handles GET /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs
func (h *QueueHandlers) ListDLQJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse max_jobs query parameter
	maxJobs := 100 // Default
	if maxJobsStr := r.URL.Query().Get("max_jobs"); maxJobsStr != "" {
		if val, err := strconv.Atoi(maxJobsStr); err == nil && val > 0 {
			maxJobs = val
		}
	}
	if maxJobs > 100 {
		maxJobs = 100
	}

	// List DLQ jobs
	ctx := r.Context()
	queueMgr := h.storage.QueueManager()
	dlqJobs, err := queueMgr.ListDLQJobs(ctx, resourcePath, maxJobs)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Get DLQ path to read jobs from
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Convert to response format
	jobs := make([]*Job, 0, len(dlqJobs))
	for _, job := range dlqJobs {
		payload, err := queueMgr.GetJobPayload(ctx, dlqPath, job.ID)
		if err != nil {
			continue // Skip jobs we can't read
		}

		jobs = append(jobs, &Job{
			ID:        job.ID,
			Seq:       job.Seq,
			Payload:   payload,
			Attempts:  job.Attempts,
			CreatedAt: job.CreatedAt.Unix(),
		})
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ListDLQJobsResponse{
		Status: "ok",
		Jobs:   jobs,
	})
}

// ReplayDLQJob handles POST /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs/{job_id}/replay
func (h *QueueHandlers) ReplayDLQJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractQueuePathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extract job_id from path (format: /dlq/jobs/{job_id}/replay)
	pathParts := strings.Split(r.URL.Path, "/")
	jobID := ""
	for i, part := range pathParts {
		if part == "jobs" && i+1 < len(pathParts) {
			jobID = pathParts[i+1]
			break
		}
	}
	if jobID == "" {
		http.Error(w, "job_id is required", http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "queue", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get DLQ path
	ctx := r.Context()
	queueMgr := h.storage.QueueManager()
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Get job payload from DLQ
	payload, err := queueMgr.GetJobPayload(ctx, dlqPath, jobID)
	if err != nil {
		h.writeError(w, err, dlqPath)
		return
	}

	// Enqueue back to main queue
	newJobID, seq, err := queueMgr.Enqueue(ctx, resourcePath, payload, storage.QueueEnqueueOptions{})
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ReplayDLQJobResponse{
		Status:  "ok",
		Message: "job replayed successfully",
		JobID:   newJobID,
		Seq:     seq,
	})
}
