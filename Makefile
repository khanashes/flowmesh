.PHONY: help build test lint clean dev run version install-tools verify docker-build docker-run docker-stop docker-clean docker-push

# Variables
BINARY_NAME=flowmesh
ENGINE_DIR=engine
BIN_DIR=bin
VERSION?=$(shell cat VERSION 2>/dev/null || echo "dev")

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLINT=golangci-lint

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the FlowMesh binary
	@echo "Building FlowMesh..."
	@mkdir -p $(BIN_DIR)
	@cd $(ENGINE_DIR) && \
		$(GOBUILD) -o ../$(BIN_DIR)/$(BINARY_NAME) \
		-ldflags "-X github.com/flowmesh/engine/internal/version.Version=$(VERSION)" \
		./cmd/flowmesh
	@echo "Binary built: $(BIN_DIR)/$(BINARY_NAME)"

test: ## Run tests
	@echo "Running tests..."
	@cd $(ENGINE_DIR) && $(GOTEST) -v -race -coverprofile=coverage.out ./...
	@cd $(ENGINE_DIR) && $(GOTEST) -v ./...

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	@cd $(ENGINE_DIR) && $(GOTEST) -v -race -coverprofile=coverage.out ./...
	@cd $(ENGINE_DIR) && $(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: $(ENGINE_DIR)/coverage.html"

lint: ## Run linters
	@echo "Running linters..."
	@cd $(ENGINE_DIR) && $(GOLINT) run ./...

fmt: ## Format code
	@echo "Formatting code..."
	@cd $(ENGINE_DIR) && $(GOFMT) -w .
	@echo "Code formatted"

fmt-check: ## Check code formatting
	@echo "Checking code formatting..."
	@cd $(ENGINE_DIR) && $(GOFMT) -l . | grep -q . && exit 1 || exit 0

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@cd $(ENGINE_DIR) && rm -f coverage.out coverage.html
	@find . -type d -name "data" -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete"

dev: build ## Build and run in development mode
	@echo "Starting FlowMesh in development mode..."
	@./$(BIN_DIR)/$(BINARY_NAME) --log-level debug --log-format text

run: build ## Build and run
	@./$(BIN_DIR)/$(BINARY_NAME)

version: ## Show version information
	@echo "Version: $(VERSION)"
	@cd $(ENGINE_DIR) && $(GOCMD) run ./cmd/flowmesh --version 2>/dev/null || echo "Run 'make build' first"

install-tools: ## Install development tools
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Tools installed"

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	@cd $(ENGINE_DIR) && $(GOMOD) download
	@cd $(ENGINE_DIR) && $(GOMOD) tidy
	@echo "Dependencies downloaded"

verify: ## Verify development environment
	@echo "Verifying development environment..."
	@command -v go >/dev/null 2>&1 || { echo "Go is not installed"; exit 1; }
	@go version
	@echo "✓ Go is installed"
	@cd $(ENGINE_DIR) && $(GOMOD) verify || { echo "Dependencies verification failed"; exit 1; }
	@echo "✓ Dependencies are valid"
	@echo "Environment verification complete"

# Docker targets
docker-build: ## Build Docker image
	@echo "Building Docker image..."
	@docker build -t flowmesh:$(VERSION) \
		--build-arg VERSION=$(VERSION) \
		-f docker/Dockerfile .
	@echo "Docker image built: flowmesh:$(VERSION)"

docker-run: docker-build ## Build and run Docker container
	@echo "Running Docker container..."
	@docker-compose up -d
	@echo "Container started. Use 'docker-compose logs -f' to view logs"

docker-dev: ## Run Docker container in development mode
	@echo "Running Docker container in development mode..."
	@docker-compose -f docker-compose.dev.yml up -d
	@echo "Development container started. Use 'docker-compose -f docker-compose.dev.yml logs -f' to view logs"

docker-stop: ## Stop Docker container
	@echo "Stopping Docker containers..."
	@docker-compose down
	@docker-compose -f docker-compose.dev.yml down 2>/dev/null || true
	@echo "Containers stopped"

docker-logs: ## View Docker container logs
	@docker-compose logs -f

docker-clean: ## Clean Docker images and containers
	@echo "Cleaning Docker resources..."
	@docker-compose down -v 2>/dev/null || true
	@docker-compose -f docker-compose.dev.yml down -v 2>/dev/null || true
	@docker rmi flowmesh:$(VERSION) 2>/dev/null || true
	@docker rmi flowmesh:latest 2>/dev/null || true
	@docker rmi flowmesh:dev 2>/dev/null || true
	@echo "Docker resources cleaned"

docker-push: docker-build ## Build and push Docker image to registry
	@echo "Pushing Docker image..."
	@docker tag flowmesh:$(VERSION) flowmesh:latest
	@echo "Tagged as latest. Run 'docker push <registry>/flowmesh:$(VERSION)' to push"

