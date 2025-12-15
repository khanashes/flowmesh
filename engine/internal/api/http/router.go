package http

import (
	"net/http"
	"strings"

	"github.com/flowmesh/engine/internal/api/http/handlers"
	"github.com/flowmesh/engine/internal/api/http/middleware"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Router manages HTTP routes and middleware
type Router struct {
	mux            *http.ServeMux
	storage        storage.StorageBackend
	streamHandlers *handlers.StreamHandlers
	queueHandlers  *handlers.QueueHandlers
	kvHandlers     *handlers.KVHandlers
	schemaHandlers *handlers.SchemaHandlers
	metricsHandler http.Handler
}

// NewRouter creates a new router
func NewRouter(storage storage.StorageBackend) *Router {
	r := &Router{
		mux:            http.NewServeMux(),
		storage:        storage,
		streamHandlers: handlers.NewStreamHandlers(storage),
		queueHandlers:  handlers.NewQueueHandlers(storage),
		kvHandlers:     handlers.NewKVHandlers(storage),
		schemaHandlers: handlers.NewSchemaHandlers(storage),
	}

	// Set up metrics handler with custom registry if available
	if storage != nil {
		if metricsCollector := storage.MetricsCollector(); metricsCollector != nil {
			if registry := metricsCollector.GetRegistry(); registry != nil {
				r.metricsHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
			} else {
				r.metricsHandler = promhttp.Handler()
			}
		} else {
			r.metricsHandler = promhttp.Handler()
		}
	} else {
		r.metricsHandler = promhttp.Handler()
	}

	r.setupRoutes()

	return r
}

// setupRoutes sets up all HTTP routes
func (r *Router) setupRoutes() {
	// Create middleware chain
	chain := middleware.Chain(
		middleware.Recovery(logger.WithComponent("http.middleware")),
		middleware.Logging(logger.WithComponent("http.middleware")),
		// Auth middleware will be added here when needed
	)

	// Health check endpoints (no auth required)
	r.mux.Handle("/health", chain(http.HandlerFunc(handlers.HealthCheck)))
	r.mux.Handle("/ready", chain(http.HandlerFunc(handlers.ReadinessCheck(r.storage))))

	// Metrics endpoint (no auth required, standard Prometheus pattern)
	r.mux.Handle("/metrics", r.metricsHandler)

	// Stream API endpoints
	r.mux.Handle("/api/v1/streams/", chain(http.HandlerFunc(r.handleStreamRoutes)))

	// Queue API endpoints
	r.mux.Handle("/api/v1/queues/", chain(http.HandlerFunc(r.handleQueueRoutes)))

	// KV API endpoints
	r.mux.Handle("/api/v1/kv/", chain(http.HandlerFunc(r.handleKVRoutes)))

	// Schema API endpoints
	r.mux.Handle("/api/v1/schemas/", chain(http.HandlerFunc(r.handleSchemaRoutes)))

	// Default API v1 route (for unmatched paths)
	r.mux.Handle("/api/v1/", chain(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.NotFound(w, req)
	})))
}

// handleStreamRoutes routes stream-related requests to appropriate handlers
func (r *Router) handleStreamRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// POST /api/v1/streams/{tenant}/{namespace}/{name}/events
	if req.Method == http.MethodPost && matchPattern(path, "/events") {
		r.streamHandlers.WriteEvents(w, req)
		return
	}

	// GET /api/v1/streams/{tenant}/{namespace}/{name}/messages
	if req.Method == http.MethodGet && matchPattern(path, "/messages") {
		r.streamHandlers.ReadStream(w, req)
		return
	}

	// GET /api/v1/streams/{tenant}/{namespace}/{name}/offsets/latest
	if req.Method == http.MethodGet && matchPattern(path, "/offsets/latest") {
		r.streamHandlers.GetLatestOffset(w, req)
		return
	}

	// POST /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets
	if req.Method == http.MethodPost && matchPattern(path, "/consumer-groups/", "/offsets") {
		r.streamHandlers.CommitOffset(w, req)
		return
	}

	// GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/offsets
	if req.Method == http.MethodGet && matchPattern(path, "/consumer-groups/", "/offsets") {
		r.streamHandlers.GetOffset(w, req)
		return
	}

	// GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups/{group}/state
	if req.Method == http.MethodGet && matchPattern(path, "/consumer-groups/", "/state") {
		r.streamHandlers.GetConsumerGroupState(w, req)
		return
	}

	// No match found
	http.NotFound(w, req)
}

// handleQueueRoutes routes queue-related requests to appropriate handlers
func (r *Router) handleQueueRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs
	if req.Method == http.MethodPost && matchPattern(path, "/jobs") {
		r.queueHandlers.Enqueue(w, req)
		return
	}

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/reserve
	if req.Method == http.MethodPost && matchPattern(path, "/reserve") {
		r.queueHandlers.Reserve(w, req)
		return
	}

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/receive
	if req.Method == http.MethodPost && matchPattern(path, "/receive") {
		r.queueHandlers.Receive(w, req)
		return
	}

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/ack
	if req.Method == http.MethodPost && matchPattern(path, "/jobs/", "/ack") {
		r.queueHandlers.ACK(w, req)
		return
	}

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/jobs/{job_id}/nack
	if req.Method == http.MethodPost && matchPattern(path, "/jobs/", "/nack") {
		r.queueHandlers.NACK(w, req)
		return
	}

	// GET /api/v1/queues/{tenant}/{namespace}/{name}/stats
	if req.Method == http.MethodGet && matchPattern(path, "/stats") {
		r.queueHandlers.GetQueueStats(w, req)
		return
	}

	// PUT /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
	if req.Method == http.MethodPut && matchPattern(path, "/retry-policy") {
		r.queueHandlers.SetRetryPolicy(w, req)
		return
	}

	// GET /api/v1/queues/{tenant}/{namespace}/{name}/retry-policy
	if req.Method == http.MethodGet && matchPattern(path, "/retry-policy") {
		r.queueHandlers.GetRetryPolicy(w, req)
		return
	}

	// GET /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs
	if req.Method == http.MethodGet && matchPattern(path, "/dlq/jobs") {
		r.queueHandlers.ListDLQJobs(w, req)
		return
	}

	// POST /api/v1/queues/{tenant}/{namespace}/{name}/dlq/jobs/{job_id}/replay
	if req.Method == http.MethodPost && matchPattern(path, "/dlq/jobs/", "/replay") {
		r.queueHandlers.ReplayDLQJob(w, req)
		return
	}

	// No match found
	http.NotFound(w, req)
}

// handleKVRoutes routes KV-related requests to appropriate handlers
func (r *Router) handleKVRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// PUT /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
	if req.Method == http.MethodPut && matchPattern(path, "/keys/") {
		r.kvHandlers.Set(w, req)
		return
	}

	// GET /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
	if req.Method == http.MethodGet && matchPattern(path, "/keys/") {
		r.kvHandlers.Get(w, req)
		return
	}

	// DELETE /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
	if req.Method == http.MethodDelete && matchPattern(path, "/keys/") {
		r.kvHandlers.Delete(w, req)
		return
	}

	// HEAD /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
	if req.Method == http.MethodHead && matchPattern(path, "/keys/") {
		r.kvHandlers.Exists(w, req)
		return
	}

	// GET /api/v1/kv/{tenant}/{namespace}/{name}/keys
	if req.Method == http.MethodGet && matchPattern(path, "/keys") {
		r.kvHandlers.ListKeys(w, req)
		return
	}

	// No match found
	http.NotFound(w, req)
}

// handleSchemaRoutes routes schema-related requests to appropriate handlers
func (r *Router) handleSchemaRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// POST /api/v1/schemas (or /api/v1/{tenant}/schemas)
	if req.Method == http.MethodPost {
		r.schemaHandlers.RegisterSchema(w, req)
		return
	}

	// GET /api/v1/schemas (list all schemas)
	if req.Method == http.MethodGet && !strings.Contains(path, "/v") {
		r.schemaHandlers.ListSchemas(w, req)
		return
	}

	// GET /api/v1/schemas/{schema_id}/v{version} (get specific schema)
	if req.Method == http.MethodGet && strings.Contains(path, "/v") {
		r.schemaHandlers.GetSchema(w, req)
		return
	}

	// DELETE /api/v1/schemas/{schema_id}/v{version}
	if req.Method == http.MethodDelete {
		r.schemaHandlers.DeleteSchema(w, req)
		return
	}

	// No match found
	http.NotFound(w, req)
}

// handleStreamSchemaRoutes handles schema attachment for streams
func (r *Router) handleStreamSchemaRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// PUT /api/v1/streams/{tenant}/{namespace}/{name}/schema
	if req.Method == http.MethodPut && matchPattern(path, "/schema") {
		r.schemaHandlers.SetResourceSchema(w, req)
		return
	}

	// Delegate to stream routes
	r.handleStreamRoutes(w, req)
}

// handleQueueSchemaRoutes handles schema attachment for queues
func (r *Router) handleQueueSchemaRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// PUT /api/v1/queues/{tenant}/{namespace}/{name}/schema
	if req.Method == http.MethodPut && matchPattern(path, "/schema") {
		r.schemaHandlers.SetResourceSchema(w, req)
		return
	}

	// Delegate to queue routes
	r.handleQueueRoutes(w, req)
}

// matchPattern checks if a path matches a pattern with required segments
// For example: matchPattern("/api/v1/streams/tenant/ns/name/consumer-groups/group/offsets", "/consumer-groups/", "/offsets")
// returns true if the path contains both segments in order
func matchPattern(path string, segments ...string) bool {
	if len(segments) == 0 {
		return true
	}

	lastIndex := 0
	for _, segment := range segments {
		idx := findSegment(path, segment, lastIndex)
		if idx == -1 {
			return false
		}
		lastIndex = idx + len(segment)
	}

	return true
}

// findSegment finds a segment in a path starting from a given index
func findSegment(path, segment string, startIndex int) int {
	if startIndex >= len(path) {
		return -1
	}
	subPath := path[startIndex:]
	for i := 0; i <= len(subPath)-len(segment); i++ {
		if subPath[i:i+len(segment)] == segment {
			return startIndex + i
		}
	}
	return -1
}
