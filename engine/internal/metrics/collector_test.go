package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector()
	require.NotNil(t, collector)
	assert.NotNil(t, collector.GetRegistry())
}

func TestRegisterCounter(t *testing.T) {
	collector := NewCollector()
	counter := collector.RegisterCounter("test_counter", "Test counter", []string{"label1"})
	require.NotNil(t, counter)

	// Verify it's registered
	registry := collector.GetRegistry()
	err := registry.Register(counter)
	// Should fail because it's already registered
	assert.Error(t, err)
}

func TestRegisterGauge(t *testing.T) {
	collector := NewCollector()
	gauge := collector.RegisterGauge("test_gauge", "Test gauge", []string{"label1"})
	require.NotNil(t, gauge)

	// Verify it's registered
	registry := collector.GetRegistry()
	err := registry.Register(gauge)
	// Should fail because it's already registered
	assert.Error(t, err)
}

func TestRegisterHistogram(t *testing.T) {
	collector := NewCollector()
	buckets := []float64{0.1, 0.5, 1.0, 2.5, 5.0}
	histogram := collector.RegisterHistogram("test_histogram", "Test histogram", []string{"label1"}, buckets)
	require.NotNil(t, histogram)

	// Verify it's registered
	registry := collector.GetRegistry()
	err := registry.Register(histogram)
	// Should fail because it's already registered
	assert.Error(t, err)
}

func TestRegisterHistogram_DefaultBuckets(t *testing.T) {
	collector := NewCollector()
	histogram := collector.RegisterHistogram("test_histogram_default", "Test histogram", []string{"label1"}, nil)
	require.NotNil(t, histogram)

	// Verify it's registered
	registry := collector.GetRegistry()
	err := registry.Register(histogram)
	// Should fail because it's already registered
	assert.Error(t, err)
}

func TestGetRegistry(t *testing.T) {
	collector := NewCollector()
	registry := collector.GetRegistry()
	require.NotNil(t, registry)

	// Verify it's a valid Prometheus registry (Registry implements Gatherer)
	// Should be able to gather metrics (even if empty)
	_, err := registry.Gather()
	assert.NoError(t, err)
}
