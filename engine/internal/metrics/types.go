package metrics

// Metric name constants following Prometheus naming conventions
// Format: flowmesh_{component}_{metric}_{unit}

// Queue metrics
const (
	MetricQueueDepth           = "flowmesh_queue_depth"
	MetricQueuePending         = "flowmesh_queue_pending"
	MetricQueueInFlight        = "flowmesh_queue_in_flight"
	MetricQueueTotalJobs       = "flowmesh_queue_total_jobs"
	MetricQueueCompletedJobs   = "flowmesh_queue_completed_jobs"
	MetricQueueFailedJobs      = "flowmesh_queue_failed_jobs"
	MetricQueueDLQJobs         = "flowmesh_queue_dlq_jobs"
	MetricQueueOldestJobAge    = "flowmesh_queue_oldest_job_age_seconds"
	MetricQueueEnqueueDuration = "flowmesh_queue_enqueue_duration_seconds"
	MetricQueueReserveDuration = "flowmesh_queue_reserve_duration_seconds"
)

// Stream metrics
const (
	MetricStreamOffset            = "flowmesh_stream_offset"
	MetricStreamEventsTotal       = "flowmesh_stream_events_total"
	MetricStreamWriteDuration     = "flowmesh_stream_write_duration_seconds"
	MetricStreamReadDuration      = "flowmesh_stream_read_duration_seconds"
	MetricStreamBytesWrittenTotal = "flowmesh_stream_bytes_written_total"
	MetricStreamBytesReadTotal    = "flowmesh_stream_bytes_read_total"
)

// Consumer group metrics
const (
	MetricConsumerGroupLag             = "flowmesh_consumer_group_lag"
	MetricConsumerGroupCommittedOffset = "flowmesh_consumer_group_committed_offset"
	MetricConsumerGroupOffsetCommits   = "flowmesh_consumer_group_offset_commits_total"
)

// Node-level metrics
const (
	MetricResourcesTotal           = "flowmesh_resources_total"
	MetricOperationsTotal          = "flowmesh_operations_total"
	MetricAPIRequestsTotal         = "flowmesh_api_requests_total"
	MetricAPIRequestDuration       = "flowmesh_api_request_duration_seconds"
	MetricStorageSizeBytes         = "flowmesh_storage_size_bytes"
	MetricSchemaRegistrationsTotal = "flowmesh_schema_registrations_total"
)

// Label name constants
const (
	LabelTenant        = "tenant"
	LabelNamespace     = "namespace"
	LabelQueue         = "queue"
	LabelStream        = "stream"
	LabelPartition     = "partition"
	LabelConsumerGroup = "consumer_group"
	LabelResourceType  = "resource_type"
	LabelOperation     = "operation"
	LabelStatus        = "status"
	LabelMethod        = "method"
	LabelEndpoint      = "endpoint"
	LabelComponent     = "component"
)
