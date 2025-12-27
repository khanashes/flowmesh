# FlowMesh Deployment Guide

This guide covers deploying FlowMesh in production environments.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Deployment Methods](#deployment-methods)
- [Configuration](#configuration)
- [Health Checks](#health-checks)
- [Monitoring](#monitoring)
- [Backup and Restore](#backup-and-restore)
- [Security](#security)
- [Production Checklist](#production-checklist)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

- **OS**: Linux (recommended), macOS, or Windows
- **CPU**: 2+ cores recommended
- **Memory**: 2GB+ RAM (4GB+ for production)
- **Disk**: 10GB+ free space (depends on data volume)
- **Network**: Ports 50051 (gRPC), 8080 (HTTP), 9090 (Metrics) available

### Software Requirements

- **Docker**: 20.10+ (for containerized deployment)
- **Go**: 1.24+ (for building from source)
- **Node.js**: 20.19+ or 22.12+ (for building Web UI from source)

## Quick Start

### Docker (Recommended)

```bash
# Clone repository
git clone https://github.com/flowmesh/flowmesh.git
cd flowmesh

# Start FlowMesh
docker-compose up -d

# Verify it's running
curl http://localhost:8080/health
```

### Binary

```bash
# Build from source
cd engine
go build -o ../bin/flowmesh ./cmd/flowmesh

# Run
./bin/flowmesh
```

## Deployment Methods

### Option 1: Docker Compose (Recommended for Single Node)

Best for: Development, staging, small production deployments

#### Basic Setup

```bash
docker-compose up -d
```

#### Custom Configuration

Edit `docker-compose.yml`:

```yaml
services:
  flowmesh:
    environment:
      - SERVER_GRPC_ADDR=:50051
      - SERVER_HTTP_ADDR=:8080
      - STORAGE_DATA_DIR=/app/data
      - STORAGE_FSYNC_POLICY=always  # Production: use "always" for durability
      - LOGGING_LEVEL=info
      - LOGGING_FORMAT=json
    volumes:
      - /path/to/persistent/data:/app/data  # Use host path for persistence
```

#### Production Docker Compose

```yaml
version: '3.8'

services:
  flowmesh:
    image: flowmesh:v0.1.0
    container_name: flowmesh
    restart: always
    ports:
      - "50051:50051"
      - "8080:8080"
      - "9090:9090"
    environment:
      - SERVER_GRPC_ADDR=:50051
      - SERVER_HTTP_ADDR=:8080
      - STORAGE_DATA_DIR=/app/data
      - STORAGE_FSYNC_POLICY=always
      - LOGGING_LEVEL=info
      - LOGGING_FORMAT=json
      - LOGGING_OUTPUT=/app/logs/flowmesh.log
    volumes:
      - /var/lib/flowmesh/data:/app/data
      - /var/lib/flowmesh/logs:/app/logs
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "8080"]
      interval: 30s
      timeout: 3s
      retries: 3
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G

volumes:
  flowmesh-data:
    driver: local
```

### Option 2: Standalone Binary

Best for: Custom deployments, integration with existing infrastructure

#### Build Binary

```bash
cd engine
go build -o flowmesh ./cmd/flowmesh
```

#### Run with Environment Variables

```bash
export SERVER_GRPC_ADDR=:50051
export SERVER_HTTP_ADDR=:8080
export STORAGE_DATA_DIR=/var/lib/flowmesh/data
export STORAGE_FSYNC_POLICY=always
export LOGGING_LEVEL=info
export LOGGING_FORMAT=json

./flowmesh
```

#### Run with Command Line Flags

```bash
./flowmesh \
  --grpc-addr :50051 \
  --http-addr :8080 \
  --data-dir /var/lib/flowmesh/data \
  --log-level info \
  --log-format json
```

### Option 3: Systemd Service

Best for: Linux servers, production deployments

#### Create Service File

Create `/etc/systemd/system/flowmesh.service`:

```ini
[Unit]
Description=FlowMesh Unified Event Fabric
After=network.target

[Service]
Type=simple
User=flowmesh
Group=flowmesh
WorkingDirectory=/opt/flowmesh
ExecStart=/opt/flowmesh/bin/flowmesh \
  --grpc-addr :50051 \
  --http-addr :8080 \
  --data-dir /var/lib/flowmesh/data \
  --log-level info \
  --log-format json

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/flowmesh

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Restart policy
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

#### Setup User and Directories

```bash
# Create user
sudo useradd -r -s /bin/false flowmesh

# Create directories
sudo mkdir -p /opt/flowmesh/bin
sudo mkdir -p /var/lib/flowmesh/data
sudo mkdir -p /var/lib/flowmesh/logs

# Copy binary
sudo cp flowmesh /opt/flowmesh/bin/
sudo chown -R flowmesh:flowmesh /opt/flowmesh
sudo chown -R flowmesh:flowmesh /var/lib/flowmesh

# Set permissions
sudo chmod 755 /opt/flowmesh/bin/flowmesh
sudo chmod 750 /var/lib/flowmesh
```

#### Start Service

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable service
sudo systemctl enable flowmesh

# Start service
sudo systemctl start flowmesh

# Check status
sudo systemctl status flowmesh

# View logs
sudo journalctl -u flowmesh -f
```

### Option 4: Kubernetes (Coming Soon)

Kubernetes deployment manifests are planned for future releases.

## Configuration

### Environment Variables

FlowMesh supports configuration via environment variables. All variables are prefixed with their category:

#### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_GRPC_ADDR` | `:50051` | gRPC server address |
| `SERVER_HTTP_ADDR` | `:8080` | HTTP server address |
| `SERVER_TLS_ENABLED` | `false` | Enable TLS |
| `SERVER_TLS_CERT_FILE` | - | TLS certificate file path |
| `SERVER_TLS_KEY_FILE` | - | TLS key file path |

#### Storage Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_DATA_DIR` | `./data` | Data directory path |
| `STORAGE_PERSIST` | `true` | Enable persistence |
| `STORAGE_FSYNC_POLICY` | `batch` | Fsync policy: `always`, `batch`, or `interval` |
| `STORAGE_FSYNC_INTERVAL` | `10ms` | Fsync interval (for interval policy) |

**Fsync Policies:**
- `always`: Fsync after every write (maximum durability, lower performance)
- `batch`: Fsync in batches (balanced)
- `interval`: Fsync at fixed intervals (higher performance, lower durability)

#### Logging Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LOGGING_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `LOGGING_FORMAT` | `json` | Log format: `json` or `text` |
| `LOGGING_OUTPUT` | `stdout` | Log output: `stdout` or file path |
| `LOGGING_ROTATION` | `true` | Enable log rotation |
| `LOGGING_MAX_SIZE` | `100` | Max log file size in MB |
| `LOGGING_MAX_BACKUPS` | `7` | Number of backup files to keep |
| `LOGGING_MAX_AGE` | `30` | Max age of log files in days |

#### Metrics Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_ENABLED` | `true` | Enable Prometheus metrics |
| `METRICS_ADDR` | `:9090` | Metrics server address |
| `METRICS_PATH` | `/metrics` | Metrics endpoint path |
| `TRACING_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `TRACING_ENDPOINT` | - | OpenTelemetry endpoint URL |

### Configuration File (Future)

Configuration file support (YAML/TOML) is planned for future releases. Currently, use environment variables or command-line flags.

### Configuration Precedence

1. Command-line flags (highest priority)
2. Environment variables
3. Configuration file (when available)
4. Default values (lowest priority)

### Production Configuration Example

```bash
# Server
export SERVER_GRPC_ADDR=:50051
export SERVER_HTTP_ADDR=:8080

# Storage (Production: use always fsync for maximum durability)
export STORAGE_DATA_DIR=/var/lib/flowmesh/data
export STORAGE_FSYNC_POLICY=always

# Logging (Production: JSON format for log aggregation)
export LOGGING_LEVEL=info
export LOGGING_FORMAT=json
export LOGGING_OUTPUT=/var/lib/flowmesh/logs/flowmesh.log
export LOGGING_ROTATION=true
export LOGGING_MAX_SIZE=100
export LOGGING_MAX_BACKUPS=7
export LOGGING_MAX_AGE=30

# Metrics (Production: enable tracing for observability)
export METRICS_ENABLED=true
export METRICS_ADDR=:9090
export TRACING_ENABLED=true
export TRACING_ENDPOINT=http://otel-collector:4317
```

## Health Checks

### HTTP Health Endpoint

```bash
# Health check
curl http://localhost:8080/health

# Readiness check
curl http://localhost:8080/ready
```

### Docker Health Check

Docker Compose automatically includes a health check:

```yaml
healthcheck:
  test: ["CMD", "nc", "-z", "localhost", "8080"]
  interval: 30s
  timeout: 3s
  retries: 3
  start_period: 10s
```

### Load Balancer Health Check

Configure your load balancer to use:

- **Health Check**: `GET http://flowmesh:8080/health`
- **Readiness Check**: `GET http://flowmesh:8080/ready`
- **Interval**: 30 seconds
- **Timeout**: 3 seconds
- **Success Threshold**: 2
- **Failure Threshold**: 3

### Kubernetes Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
  timeoutSeconds: 3
  successThreshold: 1
  failureThreshold: 3

livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 3
  successThreshold: 1
  failureThreshold: 3
```

## Monitoring

### Prometheus Metrics

FlowMesh exposes Prometheus metrics at `http://localhost:9090/metrics`.

#### Key Metrics

- `flowmesh_stream_events_written_total` - Total events written
- `flowmesh_stream_events_read_total` - Total events read
- `flowmesh_queue_jobs_enqueued_total` - Total jobs enqueued
- `flowmesh_queue_jobs_dequeued_total` - Total jobs dequeued
- `flowmesh_queue_depth` - Current queue depth
- `flowmesh_queue_in_flight` - Jobs in flight
- `flowmesh_operation_duration_seconds` - Operation latencies

#### Prometheus Configuration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'flowmesh'
    scrape_interval: 15s
    static_configs:
      - targets: ['flowmesh:9090']
        labels:
          instance: 'flowmesh-prod'
```

### Grafana Dashboard

Create a Grafana dashboard to visualize FlowMesh metrics. Key panels:

1. **Throughput**: Events written/read per second
2. **Queue Depth**: Current queue depths
3. **Consumer Lag**: Stream consumer lag
4. **Operation Latency**: P50, P95, P99 latencies
5. **Error Rate**: Failed operations per second

### OpenTelemetry Tracing

Enable tracing for distributed observability:

```bash
export TRACING_ENABLED=true
export TRACING_ENDPOINT=http://otel-collector:4317
```

### Log Aggregation

For production, send logs to a central log aggregation system:

- **ELK Stack**: Filebeat → Elasticsearch → Kibana
- **Loki**: Promtail → Loki → Grafana
- **Datadog**: Datadog agent → Datadog
- **CloudWatch**: CloudWatch Logs Agent → AWS CloudWatch

Configure FlowMesh to write JSON logs to a file:

```bash
export LOGGING_FORMAT=json
export LOGGING_OUTPUT=/var/lib/flowmesh/logs/flowmesh.log
```

## Backup and Restore

### Data Directory Structure

```
/var/lib/flowmesh/data/
├── metadata/
│   └── resources.json
├── streams/
│   └── {tenant}/{namespace}/{name}/
│       └── segment-*.log
├── queues/
│   └── {tenant}/{namespace}/{name}/
│       └── queue-*.log
├── kv/
│   └── {tenant}/{namespace}/{name}/
│       └── pebble.db
└── schemas/
    └── {tenant}/
        └── {schema_id}/
            └── v{version}.json
```

### Backup Procedure

#### Full Backup

```bash
# Stop FlowMesh (optional, but recommended for consistency)
sudo systemctl stop flowmesh

# Create backup
sudo tar -czf flowmesh-backup-$(date +%Y%m%d-%H%M%S).tar.gz \
  -C /var/lib/flowmesh data

# Restart FlowMesh
sudo systemctl start flowmesh
```

#### Online Backup (Without Stopping)

```bash
# Create snapshot (if supported by filesystem)
sudo btrfs subvolume snapshot /var/lib/flowmesh/data /var/lib/flowmesh/data-snapshot

# Backup snapshot
sudo tar -czf flowmesh-backup-$(date +%Y%m%d-%H%M%S).tar.gz \
  -C /var/lib/flowmesh data-snapshot

# Remove snapshot
sudo btrfs subvolume delete /var/lib/flowmesh/data-snapshot
```

#### Docker Volume Backup

```bash
# Backup volume
docker run --rm \
  -v flowmesh_flowmesh-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/flowmesh-backup-$(date +%Y%m%d-%H%M%S).tar.gz /data
```

### Restore Procedure

```bash
# Stop FlowMesh
sudo systemctl stop flowmesh

# Backup current data (safety)
sudo mv /var/lib/flowmesh/data /var/lib/flowmesh/data-old

# Restore from backup
sudo tar -xzf flowmesh-backup-20231228-120000.tar.gz -C /var/lib/flowmesh

# Set permissions
sudo chown -R flowmesh:flowmesh /var/lib/flowmesh/data

# Restart FlowMesh
sudo systemctl start flowmesh
```

### Automated Backups

#### Cron Job

```bash
# Add to crontab
0 2 * * * /usr/local/bin/backup-flowmesh.sh
```

Create `/usr/local/bin/backup-flowmesh.sh`:

```bash
#!/bin/bash
BACKUP_DIR=/var/backups/flowmesh
DATA_DIR=/var/lib/flowmesh/data
RETENTION_DAYS=30

mkdir -p $BACKUP_DIR

# Create backup
tar -czf $BACKUP_DIR/flowmesh-backup-$(date +%Y%m%d-%H%M%S).tar.gz -C /var/lib/flowmesh data

# Remove old backups
find $BACKUP_DIR -name "flowmesh-backup-*.tar.gz" -mtime +$RETENTION_DAYS -delete
```

## Security

### TLS/SSL Configuration

Enable TLS for production:

```bash
# Generate self-signed certificate (development only)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Production: Use certificates from CA
export SERVER_TLS_ENABLED=true
export SERVER_TLS_CERT_FILE=/etc/flowmesh/tls/cert.pem
export SERVER_TLS_KEY_FILE=/etc/flowmesh/tls/key.pem
```

### Authentication

Configure token-based authentication:

```bash
# Set auth token (example - use proper secret management)
export FLOWMESH_AUTH_TOKEN=<secure-token>
```

### Network Security

1. **Firewall Rules**: Restrict access to FlowMesh ports
2. **Reverse Proxy**: Use nginx/traefik for TLS termination
3. **VPN**: Use VPN for internal network access
4. **Private Network**: Deploy on private networks when possible

### File Permissions

```bash
# Set proper permissions
sudo chown -R flowmesh:flowmesh /var/lib/flowmesh
sudo chmod 750 /var/lib/flowmesh/data
sudo chmod 600 /var/lib/flowmesh/logs/*.log  # If containing sensitive data
```

### Secrets Management

Use secrets management systems:

- **Kubernetes Secrets**: For K8s deployments
- **HashiCorp Vault**: For centralized secrets
- **AWS Secrets Manager**: For AWS deployments
- **Environment Variables**: Inject at runtime (avoid hardcoding)

## Production Checklist

### Before Deployment

- [ ] Review and set appropriate fsync policy (`always` for maximum durability)
- [ ] Configure log rotation and retention
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Configure health checks (load balancer, Kubernetes)
- [ ] Set up automated backups
- [ ] Configure TLS certificates
- [ ] Set authentication tokens
- [ ] Review firewall rules
- [ ] Test backup and restore procedures
- [ ] Load test the deployment

### Deployment Steps

1. [ ] Provision server/container
2. [ ] Install FlowMesh binary or deploy container
3. [ ] Create data directories with correct permissions
4. [ ] Configure environment variables
5. [ ] Start FlowMesh service
6. [ ] Verify health endpoints respond
7. [ ] Verify metrics endpoint works
8. [ ] Test API endpoints
9. [ ] Configure load balancer health checks
10. [ ] Set up monitoring dashboards

### Post-Deployment

- [ ] Monitor metrics for 24 hours
- [ ] Verify backups are running
- [ ] Test failover scenarios (if applicable)
- [ ] Document deployment configuration
- [ ] Set up alerting for critical metrics

### Regular Maintenance

- [ ] Review logs weekly
- [ ] Verify backups monthly
- [ ] Update FlowMesh when new versions are released
- [ ] Monitor disk space usage
- [ ] Review and optimize fsync policy based on workload

## Troubleshooting

### Service Won't Start

1. Check logs:
   ```bash
   sudo journalctl -u flowmesh -n 50
   ```

2. Verify configuration:
   ```bash
   ./flowmesh --help
   ```

3. Check port availability:
   ```bash
   sudo netstat -tlnp | grep -E '50051|8080|9090'
   ```

### High Memory Usage

1. Check queue depths (may indicate slow consumers)
2. Review stream retention (old segments may need cleanup)
3. Adjust GC settings:
   ```bash
   export GOGC=100  # Default
   export GOGC=200  # More aggressive GC
   ```

### Slow Performance

1. Check fsync policy (use `batch` or `interval` if `always` is too slow)
2. Monitor disk I/O:
   ```bash
   iostat -x 1
   ```
3. Check network latency
4. Review operation latencies in metrics

### Data Corruption

1. Stop FlowMesh immediately
2. Restore from last known good backup
3. Review logs for errors
4. Check disk health:
   ```bash
   smartctl -a /dev/sda
   ```

### Connection Issues

1. Verify ports are accessible:
   ```bash
   curl http://localhost:8080/health
   ```
2. Check firewall rules
3. Verify network configuration
4. Check load balancer configuration

## Performance Tuning

### Fsync Policy

- **Development**: Use `interval` or `batch` for better performance
- **Production**: Use `always` for maximum durability (lower performance)

### Resource Limits

Set appropriate resource limits:

```yaml
# Docker
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 4G

# Systemd
LimitNOFILE=65536
LimitNPROC=4096
```

### Go Runtime

Tune Go runtime for your workload:

```bash
export GOMAXPROCS=4  # Match CPU cores
export GOGC=100      # GC percentage
```

## Support

For issues and questions:

- **GitHub Issues**: https://github.com/flowmesh/flowmesh/issues
- **Documentation**: See [README.md](../README.md) and [API.md](API.md)

