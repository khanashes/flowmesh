package handlers

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsHandler serves Prometheus metrics
func MetricsHandler(registry interface{ GetRegistry() interface{} }) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Use promhttp.Handler() which serves metrics from the default registry
		// If a custom registry is provided, we'll need to wrap it
		promhttp.Handler().ServeHTTP(w, r)
	}
}

// MetricsHandlerWithRegistry serves Prometheus metrics from a custom registry
func MetricsHandlerWithRegistry(registry interface{ GetRegistry() interface{} }) http.HandlerFunc {
	// For now, use default registry - will be updated when registry is passed
	return promhttp.Handler().ServeHTTP
}
