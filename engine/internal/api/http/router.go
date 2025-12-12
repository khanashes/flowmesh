package http

import (
	"net/http"

	"github.com/flowmesh/engine/internal/api/http/handlers"
	"github.com/flowmesh/engine/internal/api/http/middleware"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
)

// Router manages HTTP routes and middleware
type Router struct {
	mux     *http.ServeMux
	storage storage.StorageBackend
}

// NewRouter creates a new router
func NewRouter(storage storage.StorageBackend) *Router {
	r := &Router{
		mux:     http.NewServeMux(),
		storage: storage,
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

	// API v1 routes
	r.mux.Handle("/api/v1/", chain(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.NotFound(w, req)
	})))
}
