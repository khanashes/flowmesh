package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/flowmesh/engine/internal/storage"
)

// Status represents a status response
type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
	Details string `json:"details,omitempty"`
}

// HealthCheckResponse represents a health check response
type HealthCheckResponse struct {
	Status Status `json:"status"`
}

// ReadinessCheckResponse represents a readiness check response
type ReadinessCheckResponse struct {
	Status Status `json:"status"`
	Ready  bool   `json:"ready"`
}

// HealthCheck handles health check requests
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := HealthCheckResponse{
		Status: Status{
			Code:    http.StatusOK,
			Message: "healthy",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// ReadinessCheck returns a handler that checks if the storage is ready
func ReadinessCheck(storage storage.StorageBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ready := storage != nil && storage.Ready()

		var statusCode int
		var statusMessage string
		if ready {
			statusCode = http.StatusOK
			statusMessage = "ready"
		} else {
			statusCode = http.StatusServiceUnavailable
			statusMessage = "not ready"
		}

		response := ReadinessCheckResponse{
			Status: Status{
				Code:    statusCode,
				Message: statusMessage,
			},
			Ready: ready,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			// Failed to encode response, but we've already written the status code
			return
		}
	}
}
