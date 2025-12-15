package grpc

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flowmesh/engine/internal/tracing"
)

// tracingInterceptorWithInfo creates tracing interceptor with method info
func (s *Server) tracingInterceptorWithInfo(info *grpc.UnaryServerInfo) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, infoParam *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Use the info from closure, not the parameter (they should be the same)
		_ = infoParam
		// Extract trace context from gRPC metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		propagator := otel.GetTextMapPropagator()
		ctx = propagator.Extract(ctx, metadataCarrier(md))

		// Use method full name as span name
		spanName := info.FullMethod

		// Get tracer
		tracer := otel.Tracer("flowmesh.grpc")

		// Create span
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
		)

		// Parse service and method from full method name
		// Format: /package.Service/Method
		serviceName := ""
		methodName := info.FullMethod
		if len(info.FullMethod) > 0 && info.FullMethod[0] == '/' {
			parts := splitMethodName(info.FullMethod[1:])
			if len(parts) == 2 {
				serviceName = parts[0]
				methodName = parts[1]
			}
		}

		// Set attributes
		span.SetAttributes(
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", serviceName),
			attribute.String(tracing.AttrRPCMethod, methodName),
			attribute.String("rpc.method", info.FullMethod),
		)

		// Call handler
		resp, err := handler(ctx, req)

		// Set status based on gRPC status
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				span.SetAttributes(
					attribute.String(tracing.AttrRPCStatus, st.Code().String()),
				)
				span.SetStatus(codes.Error, st.Message())
			} else {
				span.SetStatus(codes.Error, err.Error())
			}
		} else {
			span.SetAttributes(
				attribute.String(tracing.AttrRPCStatus, "OK"),
			)
			span.SetStatus(codes.Ok, "")
		}

		// End span
		span.End()

		return resp, err
	}
}

// splitMethodName splits a method name into service and method parts
func splitMethodName(fullMethod string) []string {
	for i := len(fullMethod) - 1; i >= 0; i-- {
		if fullMethod[i] == '/' {
			return []string{fullMethod[:i], fullMethod[i+1:]}
		}
	}
	return []string{fullMethod}
}

// metadataCarrier adapts gRPC metadata to propagation.TextMapCarrier
type metadataCarrier metadata.MD

// Get returns the value associated with the passed key.
func (m metadataCarrier) Get(key string) string {
	vals := metadata.MD(m).Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// Set stores the key-value pair.
func (m metadataCarrier) Set(key, value string) {
	metadata.MD(m).Set(key, value)
}

// Keys lists the keys stored in this carrier.
func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
