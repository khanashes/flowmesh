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

	// SamplingStrategy specifies the sampling strategy: "always", "probabilistic", or "rate"
	// - "always": Sample all traces (default)
	// - "probabilistic": Sample with a probability (0.0 to 1.0)
	// - "rate": Sample at a fixed rate per second
	SamplingStrategy string

	// SamplingRate specifies the sampling rate
	// For "probabilistic": probability (0.0 to 1.0, default: 1.0)
	// For "rate": traces per second (default: 100.0)
	SamplingRate float64
}

// DefaultTracingConfig returns a default tracing configuration
func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		Enabled:          false,
		ServiceName:      "flowmesh",
		ServiceVersion:   "0.1.0",
		Endpoint:         "",
		Insecure:         false,
		Headers:          make(map[string]string),
		ExporterType:     "grpc",
		SamplingStrategy: "always",
		SamplingRate:     1.0,
	}
}
