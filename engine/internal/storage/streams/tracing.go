package streams

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

// StartWriteSpan starts a span for write operation
func StartWriteSpan(ctx context.Context, resourcePath string, partition int32, eventCount int) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.stream")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "stream.write",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrStreamName, name),
		attribute.Int(tracing.AttrPartition, int(partition)),
		attribute.Int(tracing.AttrEventCount, eventCount),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "write"),
	)
	return ctx, span
}

// StartReadSpan starts a span for read operation
func StartReadSpan(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.stream")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "stream.read",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrStreamName, name),
		attribute.Int(tracing.AttrPartition, int(partition)),
		attribute.Int64(tracing.AttrOffset, offset),
		attribute.Int("stream.max_messages", maxMessages),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "read"),
	)
	return ctx, span
}

// StartSubscribeSpan starts a span for subscribe operation
func StartSubscribeSpan(ctx context.Context, resourcePath, consumerGroup string, partition int32) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.stream")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "stream.subscribe",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrStreamName, name),
		attribute.String(tracing.AttrConsumerGroup, consumerGroup),
		attribute.Int(tracing.AttrPartition, int(partition)),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "subscribe"),
	)
	return ctx, span
}
