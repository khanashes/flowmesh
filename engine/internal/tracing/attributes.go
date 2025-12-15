package tracing

// Span attribute keys following OpenTelemetry semantic conventions
const (
	// Resource attributes
	AttrTenant       = "flowmesh.tenant"
	AttrNamespace    = "flowmesh.namespace"
	AttrResourceName = "flowmesh.resource_name"

	// Queue attributes
	AttrQueueName   = "flowmesh.queue.name"
	AttrJobID       = "flowmesh.job.id"
	AttrJobAttempts = "flowmesh.job.attempts"

	// Stream attributes
	AttrStreamName   = "flowmesh.stream.name"
	AttrPartition    = "flowmesh.partition"
	AttrOffset       = "flowmesh.offset"
	AttrEventCount   = "flowmesh.event.count"
	AttrBytesWritten = "flowmesh.bytes.written"
	AttrBytesRead    = "flowmesh.bytes.read"

	// Consumer group attributes
	AttrConsumerGroup = "flowmesh.consumer_group.name"
	AttrLag           = "flowmesh.lag"

	// KV store attributes
	AttrKVStoreName = "flowmesh.kv_store.name"
	AttrKey         = "flowmesh.key"

	// Operation attributes
	AttrOperation = "flowmesh.operation"
	AttrStatus    = "flowmesh.status"
	AttrError     = "flowmesh.error"

	// HTTP attributes (OpenTelemetry semantic conventions)
	AttrHTTPMethod       = "http.method"
	AttrHTTPRoute        = "http.route"
	AttrHTTPStatusCode   = "http.status_code"
	AttrHTTPUserAgent    = "http.user_agent"
	AttrHTTPRequestSize  = "http.request.size"
	AttrHTTPResponseSize = "http.response.size"

	// gRPC attributes (OpenTelemetry semantic conventions)
	AttrRPCService      = "rpc.service"
	AttrRPCMethod       = "rpc.method"
	AttrRPCStatus       = "rpc.status_code"
	AttrRPCRequestSize  = "rpc.request.size"
	AttrRPCResponseSize = "rpc.response.size"
)
