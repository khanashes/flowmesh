package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/rs/zerolog"
)

// Provider wraps OpenTelemetry TracerProvider
type Provider struct {
	tracerProvider *sdktrace.TracerProvider
	config         TracingConfig
	log            zerolog.Logger
}

// NewProvider creates a new tracing provider with OTLP exporter
func NewProvider(config TracingConfig) (*Provider, error) {
	if !config.Enabled {
		return &Provider{
			tracerProvider: nil,
			config:         config,
			log:            logger.WithComponent("tracing"),
		}, nil
	}

	if config.Endpoint == "" {
		return nil, fmt.Errorf("tracing endpoint is required when tracing is enabled")
	}

	// Create resource with service information
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String(config.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create exporter based on type
	var exporter sdktrace.SpanExporter
	switch config.ExporterType {
	case "http":
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(config.Endpoint),
		}
		if config.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if len(config.Headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(config.Headers))
		}
		exporter, err = otlptracehttp.New(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP OTLP exporter: %w", err)
		}
	case "grpc":
		fallthrough
	default:
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(config.Endpoint),
		}
		if config.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		if len(config.Headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(config.Headers))
		}
		exporter, err = otlptracegrpc.New(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC OTLP exporter: %w", err)
		}
	}

	// Create tracer provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // TODO: Make configurable
	)

	// Set as global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log := logger.WithComponent("tracing")
	log.Info().
		Str("service_name", config.ServiceName).
		Str("endpoint", config.Endpoint).
		Str("exporter_type", config.ExporterType).
		Msg("Tracing provider initialized")

	return &Provider{
		tracerProvider: tp,
		config:         config,
		log:            log,
	}, nil
}

// GetTracer returns a tracer instance for a component
func (p *Provider) GetTracer(name string) trace.Tracer {
	if p.tracerProvider == nil {
		return trace.NewNoopTracerProvider().Tracer(name)
	}
	return p.tracerProvider.Tracer(name)
}

// Shutdown gracefully shuts down the provider
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.tracerProvider == nil {
		return nil
	}

	// Create timeout context if not provided
	shutdownCtx := ctx
	if _, hasTimeout := ctx.Deadline(); !hasTimeout {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	p.log.Info().Msg("Shutting down tracing provider")
	if err := p.tracerProvider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("failed to shutdown tracer provider: %w", err)
	}

	p.log.Info().Msg("Tracing provider shut down")
	return nil
}

// IsEnabled returns true if tracing is enabled
func (p *Provider) IsEnabled() bool {
	return p.config.Enabled && p.tracerProvider != nil
}
