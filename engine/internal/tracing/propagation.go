package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ExtractFromHeaders extracts trace context from message headers
func ExtractFromHeaders(headers map[string]string) context.Context {
	if headers == nil {
		return context.Background()
	}

	// Create a carrier from headers map
	carrier := make(propagation.HeaderCarrier)
	for k, v := range headers {
		carrier.Set(k, v)
	}

	// Extract trace context
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	return ctx
}

// InjectToHeaders injects trace context into message headers
func InjectToHeaders(ctx context.Context, headers map[string]string) {
	if headers == nil {
		return
	}

	// Create a carrier
	carrier := make(propagation.HeaderCarrier)

	// Inject trace context
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Copy to headers map
	for k, v := range carrier {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
}

// StartChildSpan starts a child span from parent context
func StartChildSpan(ctx context.Context, tracer trace.Tracer, operationName string) (context.Context, trace.Span) {
	return tracer.Start(ctx, operationName)
}

// SpanFromContext returns the span from context if it exists
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
