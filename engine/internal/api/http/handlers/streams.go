package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/metastore"
	streamserrors "github.com/flowmesh/engine/internal/storage/streams"
)

// StreamHandlers provides HTTP handlers for stream operations
type StreamHandlers struct {
	storage storage.StorageBackend
	wsHub   *Hub // WebSocket hub for real-time updates
}

// NewStreamHandlers creates new stream handlers
func NewStreamHandlers(storage storage.StorageBackend) *StreamHandlers {
	return &StreamHandlers{
		storage: storage,
		wsHub:   nil, // Will be set by router if WebSocket is enabled
	}
}

// SetWSHub sets the WebSocket hub
func (h *StreamHandlers) SetWSHub(hub *Hub) {
	h.wsHub = hub
}

// WriteEventsRequest represents a request to write events
type WriteEventsRequest struct {
	Events []Event `json:"events"`
}

// Event represents an event
type Event struct {
	Payload []byte            `json:"payload"`
	Headers map[string]string `json:"headers,omitempty"`
}

// WriteEventsResponse represents a response to writing events
type WriteEventsResponse struct {
	Status  string  `json:"status"`
	Message string  `json:"message,omitempty"`
	Offsets []int64 `json:"offsets"`
}

// WriteEvents handles POST /api/v1/streams/{tenant}/{namespace}/{name}/events
func (h *StreamHandlers) WriteEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractStreamPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req WriteEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Events) == 0 {
		http.Error(w, "at least one event is required", http.StatusBadRequest)
		return
	}

	// Validate events
	for i, event := range req.Events {
		if len(event.Payload) == 0 {
			http.Error(w, "event "+strconv.Itoa(i)+": payload cannot be empty", http.StatusBadRequest)
			return
		}
	}

	// Convert to storage events
	events := make([]storage.StreamEvent, 0, len(req.Events))
	for _, event := range req.Events {
		events = append(events, storage.StreamEvent{
			Payload: event.Payload,
			Headers: event.Headers,
		})
	}

	// Write events
	streamMgr := h.storage.StreamManager()
	offsets, err := streamMgr.WriteEvents(r.Context(), resourcePath, events)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := WriteEventsResponse{
		Status:  "success",
		Message: "events written successfully",
		Offsets: offsets,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast stream stats update after writing events
	if h.wsHub != nil {
		// Get updated stats to broadcast
		if latestOffset, err := streamMgr.GetLatestOffset(r.Context(), resourcePath, int32(0)); err == nil {
			h.wsHub.BroadcastStreamStats(tenant, namespace, name, StreamStats{
				LatestOffset: latestOffset,
			})
		}
	}
}

// ReadStreamRequest represents query parameters for reading
type ReadStreamRequest struct {
	Partition   int32
	Offset      int64
	MaxMessages int
}

// ReadStreamResponse represents a response to reading from a stream
type ReadStreamResponse struct {
	Status   string    `json:"status"`
	Message  string    `json:"message,omitempty"`
	Messages []Message `json:"messages"`
}

// Message represents a message
type Message struct {
	ID            string            `json:"id"`
	ResourcePath  string            `json:"resource_path"`
	Partition     int32             `json:"partition"`
	Offset        int64             `json:"offset"`
	Payload       []byte            `json:"payload"`
	Headers       map[string]string `json:"headers,omitempty"`
	CreatedAt     int64             `json:"created_at"`
	SchemaVersion int32             `json:"schema_version"`
}

// ReadStream handles GET /api/v1/streams/{tenant}/{namespace}/{name}/messages
func (h *StreamHandlers) ReadStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractStreamPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse query parameters
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		http.Error(w, "offset parameter is required", http.StatusBadRequest)
		return
	}
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "invalid offset parameter", http.StatusBadRequest)
		return
	}

	maxMessages := 100
	if m := r.URL.Query().Get("max_messages"); m != "" {
		mInt, err := strconv.Atoi(m)
		if err != nil || mInt <= 0 {
			http.Error(w, "invalid max_messages parameter", http.StatusBadRequest)
			return
		}
		if mInt > 1000 {
			mInt = 1000
		}
		maxMessages = mInt
	}

	// Read messages
	streamMgr := h.storage.StreamManager()
	messages, err := streamMgr.ReadFromOffset(r.Context(), resourcePath, partition, offset, maxMessages)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Convert to response messages
	responseMessages := make([]Message, 0, len(messages))
	for _, msg := range messages {
		responseMessages = append(responseMessages, Message{
			ID:            msg.ID,
			ResourcePath:  msg.ResourcePath,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			Payload:       msg.Payload,
			Headers:       msg.Headers,
			CreatedAt:     msg.CreatedAt.UnixNano(),
			SchemaVersion: msg.SchemaVersion,
		})
	}

	// Write response
	response := ReadStreamResponse{
		Status:   "success",
		Message:  "messages read successfully",
		Messages: responseMessages,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// CommitOffsetRequest represents a request to commit an offset
type CommitOffsetRequest struct {
	Offset int64 `json:"offset"`
}

// CommitOffsetResponse represents a response to committing an offset
type CommitOffsetResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// CommitOffset handles POST /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets
func (h *StreamHandlers) CommitOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, consumerGroup, err := extractConsumerGroupPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req CommitOffsetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Offset < -1 {
		http.Error(w, "offset cannot be less than -1", http.StatusBadRequest)
		return
	}

	// Parse partition from query (default 0)
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	// Commit offset
	consumerMgr := h.storage.ConsumerGroupManager()
	err = consumerMgr.CommitOffset(r.Context(), resourcePath, consumerGroup, partition, req.Offset)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := CommitOffsetResponse{
		Status:  "success",
		Message: "offset committed successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// GetOffsetResponse represents a response to getting an offset
type GetOffsetResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Offset  int64  `json:"offset"`
}

// GetOffset handles GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets
func (h *StreamHandlers) GetOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, consumerGroup, err := extractConsumerGroupPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse partition from query (default 0)
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	// Get committed offset
	consumerMgr := h.storage.ConsumerGroupManager()
	offset, err := consumerMgr.GetCommittedOffset(r.Context(), resourcePath, consumerGroup, partition)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := GetOffsetResponse{
		Status:  "success",
		Message: "offset retrieved successfully",
		Offset:  offset,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// GetLatestOffsetResponse represents a response to getting the latest offset
type GetLatestOffsetResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Offset  int64  `json:"offset"`
}

// GetLatestOffset handles GET /api/v1/streams/{tenant}/{namespace}/{name}/offsets/latest
func (h *StreamHandlers) GetLatestOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractStreamPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse partition from query (default 0)
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	// Get latest offset
	streamMgr := h.storage.StreamManager()
	offset, err := streamMgr.GetLatestOffset(r.Context(), resourcePath, partition)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := GetLatestOffsetResponse{
		Status:  "success",
		Message: "latest offset retrieved successfully",
		Offset:  offset,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// GetConsumerGroupStateResponse represents a response to getting consumer group state
type GetConsumerGroupStateResponse struct {
	Status  string             `json:"status"`
	Message string             `json:"message,omitempty"`
	State   ConsumerGroupState `json:"state"`
}

// ConsumerGroupState represents consumer group state
type ConsumerGroupState struct {
	Stream          string `json:"stream"`
	Group           string `json:"group"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	LatestOffset    int64  `json:"latest_offset"`
	Lag             int64  `json:"lag"`
}

// GetConsumerGroupState handles GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/state
func (h *StreamHandlers) GetConsumerGroupState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, consumerGroup, err := extractConsumerGroupPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse partition from query (default 0)
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	// Get consumer group state
	consumerMgr := h.storage.ConsumerGroupManager()
	state, err := consumerMgr.GetConsumerGroupState(r.Context(), resourcePath, consumerGroup, partition)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := GetConsumerGroupStateResponse{
		Status:  "success",
		Message: "consumer group state retrieved successfully",
		State: ConsumerGroupState{
			Stream:          state.Stream,
			Group:           state.Group,
			Partition:       state.Partition,
			CommittedOffset: state.CommittedOffset,
			LatestOffset:    state.LatestOffset,
			Lag:             state.Lag,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// ListStreamsResponse represents a response to listing streams
type ListStreamsResponse struct {
	Status  string                      `json:"status"`
	Message string                      `json:"message,omitempty"`
	Streams []*metastore.ResourceConfig `json:"streams"`
}

// ListStreams handles GET /api/v1/streams
func (h *StreamHandlers) ListStreams(w http.ResponseWriter, r *http.Request) {
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

	// List stream resources
	streams, err := metaStore.ListResources(tenant, namespace, metastore.ResourceStream)
	if err != nil {
		h.writeError(w, err, "")
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(ListStreamsResponse{
		Status:  "success",
		Message: "streams retrieved successfully",
		Streams: streams,
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// StreamStats represents stream statistics
type StreamStats struct {
	LatestOffset int64 `json:"latest_offset"`
}

// StreamStatsResponse represents a response to getting stream statistics
type StreamStatsResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Stats   StreamStats `json:"stats"`
}

// GetStreamStats handles GET /api/v1/streams/{tenant}/{namespace}/{name}/stats
func (h *StreamHandlers) GetStreamStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractStreamPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse partition from query (default 0)
	partition := int32(0)
	if p := r.URL.Query().Get("partition"); p != "" {
		pInt, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition parameter", http.StatusBadRequest)
			return
		}
		partition = int32(pInt)
	}

	// Get latest offset (for partition 0 in MVP)
	streamMgr := h.storage.StreamManager()
	latestOffset, err := streamMgr.GetLatestOffset(r.Context(), resourcePath, partition)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	response := StreamStatsResponse{
		Status:  "success",
		Message: "stream statistics retrieved successfully",
		Stats: StreamStats{
			LatestOffset: latestOffset,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}

	// Broadcast stats update via WebSocket
	if h.wsHub != nil {
		h.wsHub.BroadcastStreamStats(tenant, namespace, name, response.Stats)
	}
}

// ConsumerGroupInfo represents consumer group information with state
type ConsumerGroupInfo struct {
	Group           string `json:"group"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	LatestOffset    int64  `json:"latest_offset"`
	Lag             int64  `json:"lag"`
}

// ListConsumerGroupsResponse represents a response to listing consumer groups
type ListConsumerGroupsResponse struct {
	Status         string              `json:"status"`
	Message        string              `json:"message,omitempty"`
	ConsumerGroups []ConsumerGroupInfo `json:"consumer_groups"`
}

// ListConsumerGroups handles GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups
func (h *StreamHandlers) ListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractStreamPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "stream", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get consumer group manager
	consumerMgr := h.storage.ConsumerGroupManager()

	// List consumer groups (returns group names)
	groupNames, err := consumerMgr.ListConsumerGroups(r.Context(), resourcePath)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// For each group, get state for partition 0 (MVP)
	partition := int32(0)
	consumerGroups := make([]ConsumerGroupInfo, 0, len(groupNames))
	for _, groupName := range groupNames {
		state, err := consumerMgr.GetConsumerGroupState(r.Context(), resourcePath, groupName, partition)
		if err != nil {
			// Skip groups that we can't get state for
			continue
		}

		consumerGroups = append(consumerGroups, ConsumerGroupInfo{
			Group:           state.Group,
			Partition:       state.Partition,
			CommittedOffset: state.CommittedOffset,
			LatestOffset:    state.LatestOffset,
			Lag:             state.Lag,
		})
	}

	// Write response
	response := ListConsumerGroupsResponse{
		Status:         "success",
		Message:        "consumer groups retrieved successfully",
		ConsumerGroups: consumerGroups,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// Helper functions

// extractStreamPathParams extracts tenant, namespace, and name from URL path
// Expected format: /api/v1/streams/{tenant}/{namespace}/{name}/...
func extractStreamPathParams(r *http.Request) (tenant, namespace, name string, err error) {
	// Parse URL path: /api/v1/streams/{tenant}/{namespace}/{name}/...
	path := r.URL.Path
	prefix := "/api/v1/streams/"
	if len(path) < len(prefix) {
		return "", "", "", validation.ResourcePathError{Path: path, Reason: "invalid path format"}
	}

	path = path[len(prefix):]
	parts := make([]string, 0, 3)
	current := ""
	for _, char := range path {
		if char == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
				if len(parts) >= 3 {
					break
				}
			}
		} else {
			current += string(char)
		}
	}
	if current != "" && len(parts) < 3 {
		parts = append(parts, current)
	}

	if len(parts) < 3 {
		return "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: "path must include tenant, namespace, and name"}
	}

	tenant = parts[0]
	namespace = parts[1]
	name = parts[2]

	if err := validation.ValidateResourcePathComponents(tenant, namespace, "stream", name); err != nil {
		return "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: err.Error()}
	}

	return tenant, namespace, name, nil
}

// extractConsumerGroupPathParams extracts tenant, namespace, name, and consumer group from URL path
// Expected format: /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/...
func extractConsumerGroupPathParams(r *http.Request) (tenant, namespace, name, consumerGroup string, err error) {
	// Parse URL path: /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/...
	path := r.URL.Path
	prefix := "/api/v1/streams/"
	if len(path) < len(prefix) {
		return "", "", "", "", validation.ResourcePathError{Path: path, Reason: "invalid path format"}
	}

	path = path[len(prefix):]
	parts := make([]string, 0, 5)
	current := ""
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

	// Expected: [tenant, namespace, name, "consumer-groups", group, ...]
	if len(parts) < 5 {
		return "", "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: "path must include tenant, namespace, name, and consumer group"}
	}

	if parts[3] != "consumer-groups" {
		return "", "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: "expected 'consumer-groups' in path"}
	}

	tenant = parts[0]
	namespace = parts[1]
	name = parts[2]
	consumerGroup = parts[4]

	if err := validation.ValidateNonEmpty("consumer_group", consumerGroup); err != nil {
		return "", "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: err.Error()}
	}

	if err := validation.ValidateResourcePathComponents(tenant, namespace, "stream", name); err != nil {
		return "", "", "", "", validation.ResourcePathError{Path: r.URL.Path, Reason: err.Error()}
	}

	return tenant, namespace, name, consumerGroup, nil
}

// writeError writes an error response based on the error type
func (h *StreamHandlers) writeError(w http.ResponseWriter, err error, resourcePath string) {
	statusCode := http.StatusInternalServerError
	message := err.Error()

	switch err.(type) {
	case streamserrors.StreamNotFoundError:
		statusCode = http.StatusNotFound
	case streamserrors.InvalidOffsetError:
		statusCode = http.StatusBadRequest
	case consumers.ConsumerGroupNotFoundError:
		statusCode = http.StatusNotFound
	case consumers.InvalidOffsetError:
		statusCode = http.StatusBadRequest
	case validation.ResourcePathError:
		statusCode = http.StatusBadRequest
	}

	response := map[string]interface{}{
		"status":  "error",
		"message": message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}
