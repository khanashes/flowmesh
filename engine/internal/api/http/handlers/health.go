package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/flowmesh/engine/internal/storage"
)

// HealthResponse represents a health check response
type HealthResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// HealthCheck handles health check requests
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status: "healthy",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// ReadinessCheck returns a handler that checks if the storage is ready
func ReadinessCheck(storage storage.StorageBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ready := storage != nil && storage.Ready()

		response := HealthResponse{
			Status: "ready",
		}

		if !ready {
			response.Status = "not ready"
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
