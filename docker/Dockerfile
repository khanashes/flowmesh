# Build stage for Go binary
FROM golang:1.24-alpine AS go-builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY engine/go.mod engine/go.sum ./
RUN go mod download

# Copy source code
COPY engine/ ./

# Build the binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
    -ldflags "-X github.com/flowmesh/engine/internal/version.Version=${VERSION}" \
    -o flowmesh ./cmd/flowmesh

# Build stage for Web UI
FROM node:20-alpine AS web-builder

# Set working directory
WORKDIR /build

# Copy web-ui directory
COPY web-ui/ ./

# Install dependencies
RUN npm install

# Build the web UI
RUN npm run build

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata netcat-openbsd

# Create non-root user
RUN addgroup -S flowmesh && adduser -S flowmesh -G flowmesh

WORKDIR /app

# Copy binary from Go builder
COPY --from=go-builder /build/flowmesh .

# Copy web-ui build from web builder
COPY --from=web-builder /build/dist ./web-ui/dist

# Create data directory with proper permissions
RUN mkdir -p /app/data && chown -R flowmesh:flowmesh /app

# Switch to non-root user
USER flowmesh

# Expose ports
EXPOSE 50051 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 8080 || exit 1

# Run the binary
ENTRYPOINT ["./flowmesh"]
