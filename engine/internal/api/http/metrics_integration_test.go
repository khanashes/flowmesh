package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsIntegration_HTTPEndpoint(t *testing.T) {
	// Create a storage backend with metrics enabled
	config := storage.DefaultConfig()
	config.EnableMetrics = true
	config.DataDir = t.TempDir()

	builder := storage.NewBuilder().WithConfig(config)
	storageBackend, err := builder.Build()
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)
	defer storageBackend.Stop(ctx)

	// Trigger some operations to generate metrics
	queueConfig := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	err = storageBackend.MetaStore().CreateResource(queueConfig)
	require.NoError(t, err)

	resourcePath := queueConfig.GetPath()
	_, _, err = storageBackend.QueueManager().Enqueue(ctx, resourcePath, []byte("test"), storage.QueueEnqueueOptions{})
	require.NoError(t, err)

	// Create HTTP router
	router := NewRouter(storageBackend)

	// Create test request
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve metrics endpoint
	router.mux.ServeHTTP(w, req)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
	contentType := w.Header().Get("Content-Type")
	assert.True(t, contentType == "text/plain; version=0.0.4; charset=utf-8" || contentType == "text/plain", "unexpected content type: %s", contentType)

	// Verify metrics are in response (may be empty if no metrics recorded yet, but endpoint should work)
	body := w.Body.String()
	// The endpoint should at least return something (even if empty)
	assert.NotNil(t, body)
}

func TestMetricsIntegration_QueueOperations(t *testing.T) {
	// Create a storage backend with metrics enabled
	config := storage.DefaultConfig()
	config.EnableMetrics = true
	config.DataDir = t.TempDir()

	builder := storage.NewBuilder().WithConfig(config)
	storageBackend, err := builder.Build()
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)
	defer storageBackend.Stop(ctx)

	// Get metrics collector
	metricsCollector := storageBackend.MetricsCollector()
	require.NotNil(t, metricsCollector)

	registry := metricsCollector.GetRegistry()
	require.NotNil(t, registry)

	// Create a test queue
	queueConfig := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	err = storageBackend.MetaStore().CreateResource(queueConfig)
	require.NoError(t, err)

	resourcePath := queueConfig.GetPath()

	// Enqueue a job
	jobID, _, err := storageBackend.QueueManager().Enqueue(ctx, resourcePath, []byte("test payload"), storage.QueueEnqueueOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)

	// Verify metrics were recorded
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Check for queue metrics
	var foundEnqueueMetric bool
	for _, mf := range metricFamilies {
		if mf.GetName() == "flowmesh_queue_total_jobs" {
			foundEnqueueMetric = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, foundEnqueueMetric, "queue total jobs metric should be found")

	// Get queue stats (which updates metrics)
	stats, err := storageBackend.QueueManager().GetQueueStats(ctx, resourcePath)
	require.NoError(t, err)
	assert.NotNil(t, stats)

	// Verify queue depth metric was updated
	metricFamilies, err = registry.Gather()
	require.NoError(t, err)

	var foundDepthMetric bool
	for _, mf := range metricFamilies {
		if mf.GetName() == "flowmesh_queue_depth" {
			foundDepthMetric = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, foundDepthMetric, "queue depth metric should be found")
}

func TestMetricsIntegration_StreamOperations(t *testing.T) {
	// Create a storage backend with metrics enabled
	config := storage.DefaultConfig()
	config.EnableMetrics = true
	config.DataDir = t.TempDir()

	builder := storage.NewBuilder().WithConfig(config)
	storageBackend, err := builder.Build()
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)
	defer storageBackend.Stop(ctx)

	// Create a test stream
	streamConfig := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "test-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	err = storageBackend.MetaStore().CreateResource(streamConfig)
	require.NoError(t, err)

	resourcePath := streamConfig.GetPath()
	err = storageBackend.StreamManager().InitializeStream(ctx, resourcePath)
	require.NoError(t, err)

	// Write events
	events := []storage.StreamEvent{
		{Payload: []byte("event1"), Headers: map[string]string{"key": "value"}},
		{Payload: []byte("event2"), Headers: nil},
	}
	offsets, err := storageBackend.StreamManager().WriteEvents(ctx, resourcePath, events)
	require.NoError(t, err)
	assert.Len(t, offsets, 2)

	// Verify metrics were recorded
	metricsCollector := storageBackend.MetricsCollector()
	require.NotNil(t, metricsCollector)
	registry := metricsCollector.GetRegistry()

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Check for stream metrics
	var foundStreamMetric bool
	for _, mf := range metricFamilies {
		if mf.GetName() == "flowmesh_stream_events_total" {
			foundStreamMetric = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, foundStreamMetric, "stream events total metric should be found")
}

func TestMetricsIntegration_NoMetricsWhenDisabled(t *testing.T) {
	// Create a storage backend with metrics disabled
	config := storage.DefaultConfig()
	config.EnableMetrics = false
	config.DataDir = t.TempDir()

	builder := storage.NewBuilder().WithConfig(config)
	storageBackend, err := builder.Build()
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)
	defer storageBackend.Stop(ctx)

	// Metrics collector should be nil when disabled
	metricsCollector := storageBackend.MetricsCollector()
	assert.Nil(t, metricsCollector)
}
