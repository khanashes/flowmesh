package middleware

import (
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/flowmesh/engine/internal/tracing"
)

// Tracing creates tracing middleware for HTTP requests
func Tracing() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract trace context from HTTP headers
			propagator := otel.GetTextMapPropagator()
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			// Get tracer
			tracer := otel.Tracer("flowmesh.http")

			// Create span
			spanName := "HTTP " + r.Method
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
			)

			// Set attributes
			span.SetAttributes(
				attribute.String(tracing.AttrHTTPMethod, r.Method),
				attribute.String(tracing.AttrHTTPRoute, r.URL.Path),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.scheme", r.URL.Scheme),
				attribute.String("http.host", r.Host),
				attribute.String("http.user_agent", r.UserAgent()),
				attribute.String("http.remote_addr", r.RemoteAddr),
			)

			// Wrap response writer to capture status code
			ww := &tracedResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Call next handler with context containing span
			r = r.WithContext(ctx)
			next.ServeHTTP(ww, r)

			// Set status code attribute
			span.SetAttributes(
				attribute.Int(tracing.AttrHTTPStatusCode, ww.statusCode),
			)

			// Set span status based on HTTP status code
			if ww.statusCode >= 400 {
				span.SetStatus(codes.Error, "HTTP "+strconv.Itoa(ww.statusCode))
			} else {
				span.SetStatus(codes.Ok, "")
			}

			// End span
			span.End()
		})
	}
}

// tracedResponseWriter wraps http.ResponseWriter to capture status code
type tracedResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *tracedResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
