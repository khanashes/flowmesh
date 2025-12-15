package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStreamMetrics(t *testing.T) {
	collector := NewCollector()
	metrics := NewStreamMetrics(collector)
	require.NotNil(t, metrics)
}

func TestStreamMetrics_RecordWrite(t *testing.T) {
	collector := NewCollector()
	metrics := NewStreamMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	stream := "test-stream"
	partition := int32(0)
	eventCount := 5
	bytes := int64(1024)
	duration := 100 * time.Millisecond

	metrics.RecordWrite(tenant, namespace, stream, partition, eventCount, bytes, duration)

	// Verify metrics were recorded
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Find the write duration metric
	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricStreamWriteDuration {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "write duration metric should be found")
}

func TestStreamMetrics_RecordRead(t *testing.T) {
	collector := NewCollector()
	metrics := NewStreamMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	stream := "test-stream"
	partition := int32(0)
	messageCount := 3
	bytes := int64(512)
	duration := 50 * time.Millisecond

	metrics.RecordRead(tenant, namespace, stream, partition, messageCount, bytes, duration)

	// Verify metrics were recorded
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Find the read duration metric
	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricStreamReadDuration {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "read duration metric should be found")
}

func TestStreamMetrics_UpdateOffset(t *testing.T) {
	collector := NewCollector()
	metrics := NewStreamMetrics(collector)

	tenant := "test-tenant"
	namespace := "test-ns"
	stream := "test-stream"
	partition := int32(0)
	offset := int64(100)

	metrics.UpdateOffset(tenant, namespace, stream, partition, offset)

	// Verify gauge was updated
	registry := collector.GetRegistry()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == MetricStreamOffset {
			found = true
			assert.Greater(t, len(mf.GetMetric()), 0)
			break
		}
	}
	assert.True(t, found, "stream offset gauge should be found")
}

func TestStreamMetrics_NilSafety(t *testing.T) {
	var metrics *StreamMetrics

	// Should not panic
	metrics.RecordWrite("tenant", "ns", "stream", 0, 1, 100, time.Second)
	metrics.RecordRead("tenant", "ns", "stream", 0, 1, 100, time.Second)
	metrics.UpdateOffset("tenant", "ns", "stream", 0, 100)
}
