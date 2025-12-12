package middleware

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

// Logging logs HTTP requests and responses
func Logging(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap response writer to capture status code
			ww := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			// Call next handler
			next.ServeHTTP(ww, r)

			duration := time.Since(start)

			log.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("remote_addr", r.RemoteAddr).
				Int("status", ww.statusCode).
				Dur("duration", duration).
				Msg("HTTP request")
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
