package kv

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

// StartSetSpan starts a span for Set operation
func StartSetSpan(ctx context.Context, resourcePath, key string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.kv")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "kv.set",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrKVStoreName, name),
		attribute.String(tracing.AttrKey, key),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "set"),
	)
	return ctx, span
}

// StartGetSpan starts a span for Get operation
func StartGetSpan(ctx context.Context, resourcePath, key string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.kv")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "kv.get",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrKVStoreName, name),
		attribute.String(tracing.AttrKey, key),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "get"),
	)
	return ctx, span
}

// StartDeleteSpan starts a span for Delete operation
func StartDeleteSpan(ctx context.Context, resourcePath, key string) (context.Context, trace.Span) {
	tracer := otel.Tracer("flowmesh.kv")
	tenant, namespace, name := parseResourcePath(resourcePath)
	ctx, span := tracer.Start(ctx, "kv.delete",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	span.SetAttributes(
		attribute.String(tracing.AttrTenant, tenant),
		attribute.String(tracing.AttrNamespace, namespace),
		attribute.String(tracing.AttrKVStoreName, name),
		attribute.String(tracing.AttrKey, key),
		attribute.String(tracing.AttrResourceName, resourcePath),
		attribute.String(tracing.AttrOperation, "delete"),
	)
	return ctx, span
}
