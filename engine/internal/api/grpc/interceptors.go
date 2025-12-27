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
		// Apply interceptors in order: tracing, logging, auth, error handling
		tracingInterceptor := s.tracingInterceptorWithInfo(info)
		loggingHandler := s.loggingInterceptor(s.authInterceptorWithInfo(info)(s.errorInterceptor(handler)))
		return tracingInterceptor(ctx, req, info, loggingHandler)
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

// authInterceptorWithInfo creates an auth interceptor with method info
func (s *Server) authInterceptorWithInfo(info *grpc.UnaryServerInfo) func(grpc.UnaryHandler) grpc.UnaryHandler {
	return func(handler grpc.UnaryHandler) grpc.UnaryHandler {
		return s.authInterceptor(handler, info)
	}
}

// authInterceptor authenticates requests
func (s *Server) authInterceptor(handler grpc.UnaryHandler, info *grpc.UnaryServerInfo) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		// Skip auth for health check endpoints
		if info != nil {
			methodName := info.FullMethod
			if methodName == "/flowmesh.v1.HealthService/HealthCheck" ||
				methodName == "/flowmesh.v1.HealthService/ReadinessCheck" {
				return handler(ctx, req)
			}
		}

		// Extract token from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			// No metadata, require auth (except for health checks above)
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}

		// Get authorization header
		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			// No auth header, require auth
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}

		// Extract Bearer token
		token := extractBearerToken(authHeaders[0])
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization header format")
		}

		// Validate token using token store
		if s.tokenStore != nil {
			apiToken, err := s.tokenStore.ValidateToken(token)
			if err != nil {
				return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
			}

			// Create auth context
			authCtx := &auth.AuthContext{
				TokenHash:   apiToken.TokenHash,
				Tenant:      apiToken.Tenant,
				AllowedNS:   apiToken.AllowedNS,
				Permissions: apiToken.Permissions,
			}

			// Attach auth context to request context
			ctx = auth.WithAuthContext(ctx, authCtx)
		}

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
