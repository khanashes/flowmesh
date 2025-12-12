package middleware

import (
	"net/http"
	"strings"

	"github.com/flowmesh/engine/internal/api/auth"
)

// Auth authenticates requests using API tokens
func Auth(tokenStore auth.TokenStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract token from Authorization header
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, `{"error":"missing authorization header"}`, http.StatusUnauthorized)
				return
			}

			// Extract Bearer token
			token := extractBearerToken(authHeader)
			if token == "" {
				http.Error(w, `{"error":"invalid authorization header format"}`, http.StatusUnauthorized)
				return
			}

			// Validate token
			apiToken, err := tokenStore.ValidateToken(token)
			if err != nil {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}

			// Create auth context
			authCtx := &auth.AuthContext{
				TokenHash:   apiToken.TokenHash,
				Tenant:      apiToken.Tenant,
				AllowedNS:   apiToken.AllowedNS,
				Permissions: apiToken.Permissions,
			}

			// Attach to request context
			ctx := auth.WithAuthContext(r.Context(), authCtx)
			r = r.WithContext(ctx)

			// Call next handler
			next.ServeHTTP(w, r)
		})
	}
}

// extractBearerToken extracts the Bearer token from the authorization header
func extractBearerToken(authHeader string) string {
	const bearerPrefix = "Bearer "
	if len(authHeader) < len(bearerPrefix) {
		return ""
	}
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return ""
	}
	return authHeader[len(bearerPrefix):]
}
