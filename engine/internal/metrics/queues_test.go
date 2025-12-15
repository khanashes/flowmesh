package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueueMetrics(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)
	require.NotNil(t, metrics)
}

func TestQueueMetrics_RecordEnqueue(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"
	duration := 100 * time.Millisecond

	metrics.RecordEnqueue(tenant, namespace, queue, duration)

	// Verify metrics were recorded
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Find the enqueue duration metric
	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueEnqueueDuration {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "enqueue duration metric should be found")
}

func TestQueueMetrics_RecordReserve(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"
	duration := 50 * time.Millisecond

	metrics.RecordReserve(tenant, namespace, queue, duration)

	// Verify metrics were recorded
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Find the reserve duration metric
	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueReserveDuration {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "reserve duration metric should be found")
}

func TestQueueMetrics_UpdateQueueStats(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"
	pendingJobs := int64(10)
	inFlightJobs := int64(5)
	oldestJobAgeSeconds := 30.5

	metrics.UpdateQueueStats(tenant, namespace, queue, pendingJobs, inFlightJobs, oldestJobAgeSeconds)

	// Verify metrics were updated
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Find the queue depth metric
	var foundDepth bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueDepth {
			foundDepth = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, foundDepth, "queue depth metric should be found")
}

func TestQueueMetrics_RecordJobCompleted(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"

	metrics.RecordJobCompleted(tenant, namespace, queue)
	metrics.RecordJobCompleted(tenant, namespace, queue)

	// Verify counter was incremented
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueCompletedJobs {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			// Check counter value
			for _, m := range mf.GetMetric() {
				if m.GetCounter() != nil {
					assert.GreaterOrEqual(t, m.GetCounter().GetValue(), float64(2))
				}
			}
			break
		}
	}
	assert.True(t, found, "completed jobs counter should be found")
}

func TestQueueMetrics_RecordJobFailed(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"

	metrics.RecordJobFailed(tenant, namespace, queue)

	// Verify counter was incremented
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueFailedJobs {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "failed jobs counter should be found")
}

func TestQueueMetrics_UpdateDLQJobs(t *testing.T) {
	collector := NewCollector()
	metrics := NewQueueMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	queue := "test-queue"
	count := int64(3)

	metrics.UpdateDLQJobs(tenant, namespace, queue, count)

	// Verify gauge was updated
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricQueueDLQJobs {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "DLQ jobs gauge should be found")
}

func TestQueueMetrics_NilSafety(t *testing.T) {
	var metrics *QueueMetrics

	// Should not panic
	metrics.RecordEnqueue("tenant", "ns", "queue", time.Second)
	metrics.RecordReserve("tenant", "ns", "queue", time.Second)
	metrics.UpdateQueueStats("tenant", "ns", "queue", 1, 1, 1.0)
	metrics.RecordJobCompleted("tenant", "ns", "queue")
	metrics.RecordJobFailed("tenant", "ns", "queue")
	metrics.UpdateDLQJobs("tenant", "ns", "queue", 1)
}
