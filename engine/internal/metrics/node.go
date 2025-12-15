package metrics

import (
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/prometheus/client_golang/prometheus"
)

// NodeMetrics tracks node-level metrics
type NodeMetrics struct {
	resourcesTotal           *prometheus.GaugeVec
	operationsTotal          *prometheus.CounterVec
	apiRequestsTotal         *prometheus.CounterVec
	apiRequestDuration       *prometheus.HistogramVec
	storageSizeBytes         *prometheus.GaugeVec
	schemaRegistrationsTotal *prometheus.CounterVec
}

// NewNodeMetrics initializes node-level metrics with the collector
func NewNodeMetrics(collector *Collector) *NodeMetrics {
	return &NodeMetrics{
		resourcesTotal: collector.RegisterGauge(
			MetricResourcesTotal,
			"Total number of resources by type",
			[]string{LabelResourceType},
		),
		operationsTotal: collector.RegisterCounter(
			MetricOperationsTotal,
			"Total operations by type and status",
			[]string{LabelOperation, LabelStatus},
		),
		apiRequestsTotal: collector.RegisterCounter(
			MetricAPIRequestsTotal,
			"Total HTTP/gRPC requests by method, endpoint, and status",
			[]string{LabelMethod, LabelEndpoint, LabelStatus},
		),
		apiRequestDuration: collector.RegisterHistogram(
			MetricAPIRequestDuration,
			"API request latency in seconds",
			[]string{LabelMethod, LabelEndpoint},
			prometheus.DefBuckets,
		),
		storageSizeBytes: collector.RegisterGauge(
			MetricStorageSizeBytes,
			"Storage size by component in bytes",
			[]string{LabelComponent},
		),
		schemaRegistrationsTotal: collector.RegisterCounter(
			MetricSchemaRegistrationsTotal,
			"Total number of schema registrations",
			[]string{LabelTenant},
		),
	}
}

// RecordResourceCreated increments the resource counter for the given type
func (m *NodeMetrics) RecordResourceCreated(resourceType metastore.ResourceType) {
	if m == nil {
		return
	}
	m.resourcesTotal.WithLabelValues(string(resourceType)).Inc()
}

// RecordResourceDeleted decrements the resource counter for the given type
func (m *NodeMetrics) RecordResourceDeleted(resourceType metastore.ResourceType) {
	if m == nil {
		return
	}
	m.resourcesTotal.WithLabelValues(string(resourceType)).Dec()
}

// RecordOperation records an operation with status
func (m *NodeMetrics) RecordOperation(operation, status string) {
	if m == nil {
		return
	}
	m.operationsTotal.WithLabelValues(operation, status).Inc()
}

// RecordAPIRequest records an API request
func (m *NodeMetrics) RecordAPIRequest(method, endpoint, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.apiRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
	m.apiRequestsTotal.WithLabelValues(method, endpoint, status).Inc()
}

// UpdateStorageSize updates the storage size gauge for a component
func (m *NodeMetrics) UpdateStorageSize(component string, size int64) {
	if m == nil {
		return
	}
	m.storageSizeBytes.WithLabelValues(component).Set(float64(size))
}

// RecordSchemaRegistration increments the schema registration counter
func (m *NodeMetrics) RecordSchemaRegistration(tenant string) {
	if m == nil {
		return
	}
	m.schemaRegistrationsTotal.WithLabelValues(tenant).Inc()
}
