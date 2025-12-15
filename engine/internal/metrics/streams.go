package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// StreamMetrics tracks stream-related metrics
type StreamMetrics struct {
	streamOffset            *prometheus.GaugeVec
	streamEventsTotal       *prometheus.CounterVec
	streamWriteDuration     *prometheus.HistogramVec
	streamReadDuration      *prometheus.HistogramVec
	streamBytesWrittenTotal *prometheus.CounterVec
	streamBytesReadTotal    *prometheus.CounterVec
}

// NewStreamMetrics initializes stream metrics with the collector
func NewStreamMetrics(collector *Collector) *StreamMetrics {
	return &StreamMetrics{
		streamOffset: collector.RegisterGauge(
			MetricStreamOffset,
			"Latest offset per partition",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
		),
		streamEventsTotal: collector.RegisterCounter(
			MetricStreamEventsTotal,
			"Total number of events written to stream",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
		),
		streamWriteDuration: collector.RegisterHistogram(
			MetricStreamWriteDuration,
			"Duration of WriteEvents operations in seconds",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
			prometheus.DefBuckets,
		),
		streamReadDuration: collector.RegisterHistogram(
			MetricStreamReadDuration,
			"Duration of ReadStream operations in seconds",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
			prometheus.DefBuckets,
		),
		streamBytesWrittenTotal: collector.RegisterCounter(
			MetricStreamBytesWrittenTotal,
			"Total bytes written to stream",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
		),
		streamBytesReadTotal: collector.RegisterCounter(
			MetricStreamBytesReadTotal,
			"Total bytes read from stream",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelPartition},
		),
	}
}

// RecordWrite records a write operation
func (m *StreamMetrics) RecordWrite(tenant, namespace, stream string, partition int32, eventCount int, bytes int64, duration time.Duration) {
	if m == nil {
		return
	}
	partitionStr := formatPartition(partition)
	labels := []string{tenant, namespace, stream, partitionStr}
	m.streamWriteDuration.WithLabelValues(labels...).Observe(duration.Seconds())
	m.streamEventsTotal.WithLabelValues(labels...).Add(float64(eventCount))
	m.streamBytesWrittenTotal.WithLabelValues(labels...).Add(float64(bytes))
}

// RecordRead records a read operation
func (m *StreamMetrics) RecordRead(tenant, namespace, stream string, partition int32, messageCount int, bytes int64, duration time.Duration) {
	if m == nil {
		return
	}
	partitionStr := formatPartition(partition)
	labels := []string{tenant, namespace, stream, partitionStr}
	m.streamReadDuration.WithLabelValues(labels...).Observe(duration.Seconds())
	m.streamBytesReadTotal.WithLabelValues(labels...).Add(float64(bytes))
}

// UpdateOffset updates the latest offset gauge
func (m *StreamMetrics) UpdateOffset(tenant, namespace, stream string, partition int32, offset int64) {
	if m == nil {
		return
	}
	partitionStr := formatPartition(partition)
	m.streamOffset.WithLabelValues(tenant, namespace, stream, partitionStr).Set(float64(offset))
}

// formatPartition formats partition number as string
func formatPartition(partition int32) string {
	// Convert int32 to string for label values
	return fmt.Sprintf("%d", partition)
}
