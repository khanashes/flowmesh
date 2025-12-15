package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// QueueMetrics tracks queue-related metrics
type QueueMetrics struct {
	queueDepth           *prometheus.GaugeVec
	queuePending         *prometheus.GaugeVec
	queueInFlight        *prometheus.GaugeVec
	queueTotalJobs       *prometheus.CounterVec
	queueCompletedJobs   *prometheus.CounterVec
	queueFailedJobs      *prometheus.CounterVec
	queueDLQJobs         *prometheus.GaugeVec
	queueOldestJobAge    *prometheus.GaugeVec
	queueEnqueueDuration *prometheus.HistogramVec
	queueReserveDuration *prometheus.HistogramVec
}

// NewQueueMetrics initializes queue metrics with the collector
func NewQueueMetrics(collector *Collector) *QueueMetrics {
	return &QueueMetrics{
		queueDepth: collector.RegisterGauge(
			MetricQueueDepth,
			"Current queue depth (pending + in-flight jobs)",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queuePending: collector.RegisterGauge(
			MetricQueuePending,
			"Number of pending jobs in queue",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueInFlight: collector.RegisterGauge(
			MetricQueueInFlight,
			"Number of in-flight jobs",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueTotalJobs: collector.RegisterCounter(
			MetricQueueTotalJobs,
			"Total number of jobs enqueued",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueCompletedJobs: collector.RegisterCounter(
			MetricQueueCompletedJobs,
			"Total number of successfully completed jobs",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueFailedJobs: collector.RegisterCounter(
			MetricQueueFailedJobs,
			"Total number of failed jobs",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueDLQJobs: collector.RegisterGauge(
			MetricQueueDLQJobs,
			"Number of jobs in dead-letter queue",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueOldestJobAge: collector.RegisterGauge(
			MetricQueueOldestJobAge,
			"Age of the oldest pending job in seconds",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
		),
		queueEnqueueDuration: collector.RegisterHistogram(
			MetricQueueEnqueueDuration,
			"Duration of enqueue operations in seconds",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
			prometheus.DefBuckets,
		),
		queueReserveDuration: collector.RegisterHistogram(
			MetricQueueReserveDuration,
			"Duration of reserve operations in seconds",
			[]string{LabelTenant, LabelNamespace, LabelQueue},
			prometheus.DefBuckets,
		),
	}
}

// RecordEnqueue records an enqueue operation
func (m *QueueMetrics) RecordEnqueue(tenant, namespace, queue string, duration time.Duration) {
	if m == nil {
		return
	}
	m.queueEnqueueDuration.WithLabelValues(tenant, namespace, queue).Observe(duration.Seconds())
	m.queueTotalJobs.WithLabelValues(tenant, namespace, queue).Inc()
}

// RecordReserve records a reserve operation
func (m *QueueMetrics) RecordReserve(tenant, namespace, queue string, duration time.Duration) {
	if m == nil {
		return
	}
	m.queueReserveDuration.WithLabelValues(tenant, namespace, queue).Observe(duration.Seconds())
}

// UpdateQueueStats updates gauge metrics from queue statistics
func (m *QueueMetrics) UpdateQueueStats(tenant, namespace, queue string, pendingJobs, inFlightJobs int64, oldestJobAgeSeconds float64) {
	if m == nil {
		return
	}
	labels := []string{tenant, namespace, queue}
	m.queuePending.WithLabelValues(labels...).Set(float64(pendingJobs))
	m.queueInFlight.WithLabelValues(labels...).Set(float64(inFlightJobs))
	m.queueDepth.WithLabelValues(labels...).Set(float64(pendingJobs + inFlightJobs))
	m.queueOldestJobAge.WithLabelValues(labels...).Set(oldestJobAgeSeconds)
}

// RecordJobCompleted increments the completed jobs counter
func (m *QueueMetrics) RecordJobCompleted(tenant, namespace, queue string) {
	if m == nil {
		return
	}
	m.queueCompletedJobs.WithLabelValues(tenant, namespace, queue).Inc()
}

// RecordJobFailed increments the failed jobs counter
func (m *QueueMetrics) RecordJobFailed(tenant, namespace, queue string) {
	if m == nil {
		return
	}
	m.queueFailedJobs.WithLabelValues(tenant, namespace, queue).Inc()
}

// UpdateDLQJobs updates the DLQ jobs gauge
func (m *QueueMetrics) UpdateDLQJobs(tenant, namespace, queue string, count int64) {
	if m == nil {
		return
	}
	m.queueDLQJobs.WithLabelValues(tenant, namespace, queue).Set(float64(count))
}
