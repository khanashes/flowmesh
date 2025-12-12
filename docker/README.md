# Docker Setup for FlowMesh

This directory contains Docker configuration files for containerizing FlowMesh.

## Files

- **Dockerfile**: Multi-stage build file for creating optimized FlowMesh images
- **docker-compose.yml**: Production-ready Docker Compose configuration
- **docker-compose.dev.yml**: Development Docker Compose configuration with debug settings

## Quick Start

### Build Docker Image

```bash
# Using Makefile
make docker-build

# Or using Docker directly
docker build -t flowmesh:latest -f docker/Dockerfile .
```

### Run with Docker Compose

```bash
# Production mode
docker-compose up -d

# Development mode (with debug logging)
docker-compose -f docker-compose.dev.yml up -d

# View logs
docker-compose logs -f
```

### Run with Makefile

```bash
# Build and run in production mode
make docker-run

# Run in development mode
make docker-dev

# Stop containers
make docker-stop

# View logs
make docker-logs

# Clean up
make docker-clean
```

## Image Details

### Base Images
- **Builder**: `golang:1.23-alpine` - For compiling the Go binary
- **Runtime**: `alpine:latest` - Minimal runtime image (~5MB)

### Security Features
- Non-root user execution (`flowmesh` user)
- Minimal dependencies (only ca-certificates and netcat for health checks)
- Multi-stage build to reduce image size
- No unnecessary build tools in final image

### Exposed Ports
- **50051**: gRPC API
- **8080**: HTTP/REST API
- **9090**: Metrics endpoint

### Volumes
- `/app/data`: Persistent storage for streams, queues, and metadata

## Environment Variables

### Server Configuration
- `SERVER_GRPC_ADDR`: gRPC server address (default: `:50051`)
- `SERVER_HTTP_ADDR`: HTTP server address (default: `:8080`)
- `SERVER_TLS_ENABLED`: Enable TLS (default: `false`)

### Storage Configuration
- `STORAGE_DATA_DIR`: Data directory path (default: `/app/data`)

### Logging Configuration
- `LOGGING_LEVEL`: Log level (default: `info`, options: `debug`, `info`, `warn`, `error`)
- `LOGGING_FORMAT`: Log format (default: `json`, options: `json`, `text`)
- `LOGGING_OUTPUT`: Log output (default: `stdout`, options: `stdout`, `file`)

## Health Check

The container includes a health check that verifies the HTTP server is responding:

```bash
# Check container health
docker ps
docker inspect flowmesh --format='{{.State.Health.Status}}'
```

## Building with Custom Version

```bash
# Set version during build
docker build -t flowmesh:v1.0.0 \
  --build-arg VERSION=v1.0.0 \
  -f docker/Dockerfile .

# Or using Makefile
VERSION=v1.0.0 make docker-build
```

## Data Persistence

Data is persisted in Docker volumes:

```bash
# List volumes
docker volume ls | grep flowmesh

# Inspect volume
docker volume inspect flowmesh_flowmesh-data

# Backup volume
docker run --rm -v flowmesh_flowmesh-data:/data -v $(pwd):/backup alpine tar czf /backup/flowmesh-backup.tar.gz /data

# Restore volume
docker run --rm -v flowmesh_flowmesh-data:/data -v $(pwd):/backup alpine tar xzf /backup/flowmesh-backup.tar.gz -C /
```

## Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs flowmesh

# Check container status
docker ps -a
```

### Permission issues
The container runs as non-root user `flowmesh`. Ensure data directory has correct permissions:

```bash
# Fix permissions if needed
docker exec -u root flowmesh chown -R flowmesh:flowmesh /app/data
```

### Network issues
```bash
# Test connectivity
docker exec flowmesh nc -z localhost 8080

# Check exposed ports
docker port flowmesh
```

## Production Deployment

For production deployments:

1. Use specific version tags instead of `latest`
2. Configure persistent volumes for data
3. Set up monitoring for health checks
4. Configure TLS if exposing over network
5. Use secrets management for sensitive configuration
6. Consider using orchestration tools (Kubernetes, Docker Swarm)

### Example Production docker-compose.yml

```yaml
version: '3.8'

services:
  flowmesh:
    image: flowmesh:v1.0.0  # Use specific version
    restart: always
    volumes:
      - /path/to/persistent/data:/app/data
    environment:
      - LOGGING_LEVEL=info
      - LOGGING_FORMAT=json
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G
```

