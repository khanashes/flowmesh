package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector wraps Prometheus registry and provides metric registration helpers
type Collector struct {
	registry *prometheus.Registry
}

// NewCollector creates a new metrics collector with a Prometheus registry
func NewCollector() *Collector {
	return &Collector{
		registry: prometheus.NewRegistry(),
	}
}

// RegisterCounter registers a counter metric with the collector
func (c *Collector) RegisterCounter(name, help string, labels []string) *prometheus.CounterVec {
	return promauto.With(c.registry).NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: help,
		},
		labels,
	)
}

// RegisterGauge registers a gauge metric with the collector
func (c *Collector) RegisterGauge(name, help string, labels []string) *prometheus.GaugeVec {
	return promauto.With(c.registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
		},
		labels,
	)
}

// RegisterHistogram registers a histogram metric with the collector
func (c *Collector) RegisterHistogram(name, help string, labels []string, buckets []float64) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	}
	if buckets == nil {
		// Default buckets for duration metrics (in seconds)
		opts.Buckets = prometheus.DefBuckets
	}
	return promauto.With(c.registry).NewHistogramVec(opts, labels)
}

// GetRegistry returns the Prometheus registry for HTTP handler
func (c *Collector) GetRegistry() *prometheus.Registry {
	return c.registry
}
