<!-- b332248e-f130-4953-b648-abc8b7ca893e 2ae4f9a5-878d-43b4-b579-2bd0e074f579 -->
# Phase 1: Project Foundation & Setup

This phase establishes the foundational structure for the FlowMesh Unified Event Fabric project, setting up the repository structure, development environment, and basic tooling to enable all future development work.

## Objectives

- Create clean, scalable repository structure following Go best practices
- Set up development environment with required tools and dependencies
- Initialize Go modules and project scaffolding
- Configure basic CI/CD pipeline for automated testing and builds
- Establish coding standards and project documentation templates

## Repository Structure

```
flowmesh/
├── engine/                    # Core Go engine (main server)
│   ├── cmd/
│   │   └── flowmesh/
│   │       └── main.go       # Entry point
│   ├── internal/             # Private packages
│   │   ├── storage/
│   │   ├── api/
│   │   ├── managers/
│   │   └── config/
│   ├── pkg/                  # Public packages (if needed)
│   └── go.mod
├── sdk-node/                 # Node.js SDK
│   ├── src/
│   ├── tests/
│   └── package.json
├── sdk-python/               # Python SDK
│   ├── flowmesh/
│   ├── tests/
│   └── setup.py / pyproject.toml
├── sdk-go/                   # Go SDK
│   ├── client/
│   ├── examples/
│   └── go.mod
├── web-ui/                   # React dashboard (Phase 18+)
├── schemas/                  # Schema definitions
├── examples/                 # Example applications
├── docs/                     # Documentation
├── helm/                     # Kubernetes deployment (future)
├── .github/
│   └── workflows/            # CI/CD workflows
├── docker/
│   └── Dockerfile
├── .gitignore
├── README.md
├── CONTRIBUTING.md
└── LICENSE
```

## Implementation Steps

### 1. Initialize Repository Structure

Create all directory paths following the structure above. Ensure proper separation of concerns:

- `engine/` - Core Go server application
- `sdk-*/` - Language-specific SDKs (will be implemented in later phases)
- `web-ui/` - Frontend dashboard (Phase 18+)
- `examples/` - Sample applications demonstrating usage
- `docs/` - Project documentation

### 2. Initialize Go Module for Engine

- Navigate to `engine/` directory
- Run `go mod init github.com/flowmesh/engine` (or appropriate module path)
- Set minimum Go version (recommend Go 1.21+)
- Create initial `main.go` placeholder that prints version info

### 3. Create Configuration Management

- Define configuration structure in `engine/internal/config/`
- Support for:
  - Config file (YAML/TOML)
  - Environment variables
  - CLI flags
- Configuration should include:
  - Server addresses (gRPC, HTTP)
  - Data directory paths
  - Logging level
  - Metrics/exporter settings

### 4. Set Up Logging Infrastructure

- Integrate structured logging (use `zerolog` or `logrus`)
- Configure log levels and output formats
- Set up log rotation for production use
- Ensure consistent logging across all components

### 5. Create Basic Project Documentation

- **README.md**: Project overview, quick start, architecture diagram
- **CONTRIBUTING.md**: Development setup, code style, PR process
- **LICENSE**: Choose license (suggest Apache 2.0 or MIT)
- **CHANGELOG.md**: Template for tracking changes
- **docs/ARCHITECTURE.md**: High-level system architecture

### 6. Configure Development Tools

- **.editorconfig**: Editor consistency
- **.gitignore**: Comprehensive ignore patterns for Go, Node, Python, IDE files
- **Makefile**: Common development commands
  - `make build` - Build engine binary
  - `make test` - Run tests
  - `make lint` - Run linters
  - `make dev` - Start development server (placeholder for Phase 30)
  - `make clean` - Clean build artifacts

### 7. Set Up Linting and Code Quality

- Configure `golangci-lint` for Go code
- Set up pre-commit hooks (optional but recommended)
- Define code style guidelines
- Add `.golangci.yml` configuration

### 8. Initialize CI/CD Pipeline

- Create GitHub Actions workflow in `.github/workflows/`
- **Workflow: `ci.yml`**
  - On: push to main, pull requests
  - Steps:
    - Checkout code
    - Set up Go environment
    - Run `go mod download`
    - Run linters
    - Run tests (unit tests)
    - Build binaries
    - Upload artifacts (for releases)

### 9. Create Basic Testing Infrastructure

- Set up test helpers in `engine/internal/test/`
- Create example unit test to validate setup
- Configure test coverage reporting
- Add test utilities for future use

### 10. Version Management

- Create `VERSION` file or embed version in binary
- Use build tags for version information
- Set up version command (`flowmesh version`)
- Consider using `goreleaser` configuration (future use)

### 11. Dependency Management

- Review and document key dependencies:
  - gRPC libraries
  - HTTP server (Gin, Echo, or standard library)
  - Storage engine (Pebble/Badger - decision needed)
  - Logging library
- Pin dependency versions
- Document dependency choices in `docs/DEPENDENCIES.md`

### 12. Development Environment Setup Scripts

- Create `scripts/dev-setup.sh` for initial setup
  - Verify Go version
  - Install dependencies
  - Verify tools (docker, etc.)
- Create `scripts/verify.sh` to check environment
- Document setup process in README

## Key Files to Create

### engine/cmd/flowmesh/main.go

Basic entry point that:

- Parses configuration
- Initializes logger
- Shows version information
- Has placeholder for server initialization

### engine/internal/config/config.go

Configuration struct and loading logic with:

- Server configuration (ports, addresses)
- Storage paths
- Feature flags
- Environment variable support

### .github/workflows/ci.yml

GitHub Actions workflow for continuous integration

### Makefile

Development commands for common tasks

### README.md

Comprehensive README with:

- Project description
- Quick start guide
- Development setup instructions
- Architecture overview link

## Success Criteria

- [ ] Repository structure matches planned layout
- [ ] Go module initializes and builds successfully
- [ ] Basic `flowmesh` binary compiles and runs
- [ ] Configuration system loads from file/env/flags
- [ ] Structured logging works correctly
- [ ] CI pipeline passes on push/PR
- [ ] All documentation files are in place
- [ ] Development environment can be set up via scripts
- [ ] Code quality tools (linting) are configured

## Dependencies & Prerequisites

- Go 1.21 or later installed
- Git repository initialized
- GitHub account (for CI/CD)
- Basic understanding of Go project structure

## Estimated Time

**2-3 days** for a single developer, assuming:

- Familiarity with Go ecosystem
- Clear understanding of project requirements
- Focused development time

## Notes

- This phase sets the foundation for all future work
- Keep initial code simple but extensible
- Configuration system should be flexible for future features
- Consider using Go 1.21+ features (generics where appropriate)
- Storage engine choice (Pebble vs Badger) should be documented even if not implemented yet

### To-dos

- [x] Analyze both markdown files to understand project scope and requirements
- [ ] Break down project into small, manageable phases with individual plans
- [ ] Create detailed implementation plan for each phase