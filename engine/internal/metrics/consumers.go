package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// ConsumerGroupMetrics tracks consumer group-related metrics
type ConsumerGroupMetrics struct {
	consumerGroupLag             *prometheus.GaugeVec
	consumerGroupCommittedOffset *prometheus.GaugeVec
	consumerGroupOffsetCommits   *prometheus.CounterVec
}

// NewConsumerGroupMetrics initializes consumer group metrics with the collector
func NewConsumerGroupMetrics(collector *Collector) *ConsumerGroupMetrics {
	return &ConsumerGroupMetrics{
		consumerGroupLag: collector.RegisterGauge(
			MetricConsumerGroupLag,
			"Consumer group lag per partition",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelConsumerGroup, LabelPartition},
		),
		consumerGroupCommittedOffset: collector.RegisterGauge(
			MetricConsumerGroupCommittedOffset,
			"Committed offset per partition",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelConsumerGroup, LabelPartition},
		),
		consumerGroupOffsetCommits: collector.RegisterCounter(
			MetricConsumerGroupOffsetCommits,
			"Total number of offset commits",
			[]string{LabelTenant, LabelNamespace, LabelStream, LabelConsumerGroup, LabelPartition},
		),
	}
}

// UpdateLag updates the lag gauge
func (m *ConsumerGroupMetrics) UpdateLag(tenant, namespace, stream, group string, partition int32, lag int64) {
	if m == nil {
		return
	}
	partitionStr := fmt.Sprintf("%d", partition)
	m.consumerGroupLag.WithLabelValues(tenant, namespace, stream, group, partitionStr).Set(float64(lag))
}

// UpdateCommittedOffset updates the committed offset gauge
func (m *ConsumerGroupMetrics) UpdateCommittedOffset(tenant, namespace, stream, group string, partition int32, offset int64) {
	if m == nil {
		return
	}
	partitionStr := fmt.Sprintf("%d", partition)
	m.consumerGroupCommittedOffset.WithLabelValues(tenant, namespace, stream, group, partitionStr).Set(float64(offset))
}

// RecordOffsetCommit increments the offset commit counter
func (m *ConsumerGroupMetrics) RecordOffsetCommit(tenant, namespace, stream, group string, partition int32) {
	if m == nil {
		return
	}
	partitionStr := fmt.Sprintf("%d", partition)
	m.consumerGroupOffsetCommits.WithLabelValues(tenant, namespace, stream, group, partitionStr).Inc()
}
