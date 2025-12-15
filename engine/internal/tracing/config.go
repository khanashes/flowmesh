package tracing

// TracingConfig holds configuration for OpenTelemetry tracing
type TracingConfig struct {
	// Enabled enables/disables tracing
	Enabled bool

	// ServiceName is the service name for traces
	ServiceName string

	// ServiceVersion is the service version
	ServiceVersion string

	// Endpoint is the OTLP endpoint URL
	Endpoint string

	// Insecure skips TLS verification
	Insecure bool

	// Headers contains additional headers for OTLP export
	Headers map[string]string

	// ExporterType specifies the exporter type: "grpc" or "http"
	ExporterType string
}

// DefaultTracingConfig returns a default tracing configuration
func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		Enabled:        false,
		ServiceName:    "flowmesh",
		ServiceVersion: "0.1.0",
		Endpoint:       "",
		Insecure:       false,
		Headers:        make(map[string]string),
		ExporterType:   "grpc",
	}
}
