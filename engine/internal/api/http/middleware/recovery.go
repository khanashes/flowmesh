package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/rs/zerolog"
)

// Recovery recovers from panics and returns a 500 error
func Recovery(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					log.Error().
						Interface("error", err).
						Bytes("stack", debug.Stack()).
						Msg("HTTP handler panic recovered")

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error":"internal server error"}`))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
