# Dependencies

This document describes the key dependencies used in FlowMesh and the rationale behind each choice.

## Core Runtime Dependencies

### Logging

**Package**: `github.com/rs/zerolog`
**Version**: Latest stable (v1.34.0+)
**Purpose**: Structured logging
**Rationale**:
- High performance (zero allocations in hot paths)
- Structured logging with JSON and text formats
- Contextual logging support
- Minimal dependencies
- Active maintenance

**Alternative Considered**: `logrus` - Heavier, more allocations

### Log Rotation

**Package**: `gopkg.in/natefinch/lumberjack.v2`
**Version**: v2.2.1+
**Purpose**: Log file rotation
**Rationale**:
- Simple, reliable log rotation
- Configurable size and retention
- Compression support
- Zero-downtime rotation

### Configuration

**Package**: `github.com/caarlos0/env/v11`
**Version**: v11.3.1+
**Purpose**: Environment variable parsing
**Rationale**:
- Type-safe environment variable parsing
- Support for nested structs
- Default values
- Clear error messages

**Future**: Will add YAML/TOML file support using `gopkg.in/yaml.v3` or similar

## Planned Dependencies (Phase 2+)

### gRPC

**Package**: `google.golang.org/grpc`
**Purpose**: gRPC server and client
**Status**: Will be added in Phase 2 (API Implementation)

### HTTP Server

**Options**:
- Standard library `net/http` (simple, no dependencies)
- `github.com/gin-gonic/gin` (feature-rich, fast)
- `github.com/labstack/echo` (lightweight, fast)

**Status**: Decision pending - likely standard library for Phase 1

### Storage Engine

**Options**:
- `github.com/cockroachdb/pebble` (CockroachDB's embedded KV store)
- `github.com/dgraph-io/badger` (Embedded key-value database)

**Rationale for Pebble** (preferred):
- Written in Go
- Similar to RocksDB but pure Go
- Good performance characteristics
- Active development
- Suitable for embedded use

**Rationale for Badger** (alternative):
- Pure Go implementation
- Good performance
- Widely used
- Slightly larger API surface

**Status**: Decision needed before Phase 2 (Storage Engine implementation)

### Metrics

**Package**: `github.com/prometheus/client_golang`
**Purpose**: Prometheus metrics
**Status**: Will be added in Phase 14 (Metrics & Observability)

### Tracing

**Package**: OpenTelemetry Go SDK
**Packages**:
- `go.opentelemetry.io/otel`
- `go.opentelemetry.io/otel/trace`
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace`

**Purpose**: Distributed tracing
**Status**: Will be added in Phase 15 (OpenTelemetry Tracing)

### Testing

**Package**: `github.com/stretchr/testify`
**Purpose**: Testing utilities
**Status**: Added in Phase 1 for test infrastructure

## Development Dependencies

### Linting

**Package**: `github.com/golangci/golangci-lint`
**Purpose**: Code quality and linting
**Status**: Configured in Phase 1

### Build Tools

- `Make`: Build automation
- `golangci-lint`: Code quality

## Version Management

- Dependencies are pinned in `go.mod` and `go.sum`
- Use `go get -u` carefully and test thoroughly
- Update dependencies regularly for security patches
- Document breaking changes in dependency updates

## Dependency Policy

1. **Minimize dependencies**: Prefer standard library when possible
2. **Stability**: Prefer mature, well-maintained packages
3. **Performance**: Consider performance implications
4. **License compatibility**: Ensure Apache 2.0 compatibility
5. **Maintenance**: Check activity and maintenance status

## Adding New Dependencies

Before adding a new dependency:

1. Evaluate if standard library can suffice
2. Check license compatibility
3. Review project activity and maintenance
4. Consider performance impact
5. Document rationale in this file
6. Update `go.mod` with proper version
7. Run `go mod tidy` and verify

## Security

- Regularly run `go list -m -u all` to check for updates
- Use `govulncheck` to scan for vulnerabilities
- Monitor security advisories for dependencies
- Update promptly when security patches are available

## License Compatibility

All dependencies must be compatible with Apache License 2.0:
- MIT, BSD, Apache 2.0: Compatible
- GPL: Incompatible (avoid)
- Proprietary: Incompatible (avoid)

