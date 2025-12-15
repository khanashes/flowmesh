package queues

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/flowmesh/engine/internal/tracing"
)

// parseResourcePath parses a resource path into components
// Format: tenant/namespace/resourceType/name
func parseResourcePath(resourcePath string) (tenant, namespace, name string) {
	parts := strings.Split(resourcePath, "/")
	if len(parts) >= 4 {
		return parts[0], parts[1], parts[3]
	}
	return "", "", ""
}

// StartEnqueueSpan starts a span for enqueue operation
func StartEnqueueSpan(ctx context.Context, resourcePath string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.queue")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "queue.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrQueueName, name),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "enqueue"),
	)
	return ctx, span
}

// StartReserveSpan starts a span for reserve operation
func StartReserveSpan(ctx context.Context, resourcePath string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.queue")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "queue.reserve",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrQueueName, name),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "reserve"),
	)
	return ctx, span
}

// StartACKSpan starts a span for ACK operation
func StartACKSpan(ctx context.Context, resourcePath, jobID string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.queue")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "queue.ack",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrQueueName, name),
		attribute.String(tracing.AttrJobID, jobID),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "ack"),
	)
	return ctx, span
}

// StartNACKSpan starts a span for NACK operation
func StartNACKSpan(ctx context.Context, resourcePath, jobID string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.queue")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "queue.nack",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrQueueName, name),
		attribute.String(tracing.AttrJobID, jobID),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "nack"),
	)
	return ctx, span
}
