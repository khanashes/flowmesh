package auth

import "context"

// Context key for auth context
type contextKey string

const authContextKey contextKey = "auth_context"

// WithAuthContext attaches an AuthContext to a context
func WithAuthContext(ctx context.Context, authCtx *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, authCtx)
}

// FromContext extracts an AuthContext from a context
func FromContext(ctx context.Context) (*AuthContext, bool) {
	authCtx, ok := ctx.Value(authContextKey).(*AuthContext)
	return authCtx, ok
}

// MustFromContext extracts an AuthContext from a context, panics if not found
func MustFromContext(ctx context.Context) *AuthContext {
	authCtx, ok := FromContext(ctx)
	if !ok {
		panic("auth context not found in context")
	}
	return authCtx
}
