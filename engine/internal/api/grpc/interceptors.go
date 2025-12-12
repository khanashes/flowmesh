package grpc

import (
	"context"
	"time"

	"github.com/flowmesh/engine/internal/api/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// unaryInterceptorChain creates a chain of unary interceptors
func (s *Server) unaryInterceptorChain() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Apply interceptors in order: logging, auth, error handling
		wrappedHandler := s.loggingInterceptor(s.authInterceptor(s.errorInterceptor(handler)))
		return wrappedHandler(ctx, req)
	}
}

// loggingInterceptor logs requests and responses
func (s *Server) loggingInterceptor(handler grpc.UnaryHandler) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		start := time.Now()

		// Extract metadata for logging
		md, _ := metadata.FromIncomingContext(ctx)
		log := s.log.With().
			Interface("metadata", md).
			Logger()

		log.Debug().Msg("gRPC request started")

		// Call handler
		resp, err := handler(ctx, req)

		duration := time.Since(start)
		log = log.With().Dur("duration", duration).Logger()

		if err != nil {
			log.Err(err).Msg("gRPC request failed")
		} else {
			log.Info().Msg("gRPC request completed")
		}

		return resp, err
	}
}

// authInterceptor authenticates requests
func (s *Server) authInterceptor(handler grpc.UnaryHandler) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		// Extract method from context for auth skipping
		// Note: We can't access info here, so auth skipping will be done in the actual handler
		// For now, we'll allow all requests and enforce auth in handlers

		// Extract token from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			// No metadata, continue without auth (for health checks)
			return handler(ctx, req)
		}

		// Get authorization header
		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			// No auth header, continue (for health checks)
			return handler(ctx, req)
		}

		// Extract Bearer token
		token := extractBearerToken(authHeaders[0])
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization header format")
		}

		// Validate token (for now, skip - will be implemented when we have token store)
		// TODO: Integrate with auth store when available
		_ = token

		// Create auth context (placeholder for now)
		// authCtx := &auth.AuthContext{...}
		// ctx = auth.WithAuthContext(ctx, authCtx)

		return handler(ctx, req)
	}
}

// errorInterceptor handles errors and converts them to gRPC status
func (s *Server) errorInterceptor(handler grpc.UnaryHandler) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, convertToGRPCStatus(err)
		}
		return resp, nil
	}
}

// extractBearerToken extracts the Bearer token from the authorization header
func extractBearerToken(authHeader string) string {
	const bearerPrefix = "Bearer "
	if len(authHeader) < len(bearerPrefix) {
		return ""
	}
	if authHeader[:len(bearerPrefix)] != bearerPrefix {
		return ""
	}
	return authHeader[len(bearerPrefix):]
}

// convertToGRPCStatus converts an error to a gRPC status
func convertToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's already a gRPC status
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}

	// Check for custom auth errors
	if _, ok := err.(auth.UnauthorizedError); ok {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if _, ok := err.(auth.ForbiddenError); ok {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	// Default to internal error
	return status.Error(codes.Internal, err.Error())
}
