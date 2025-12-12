.PHONY: proto generate clean

PROTO_DIR := $(shell pwd)
PROTO_FILE := flowmesh.proto
GO_OUT := ../proto/flowmeshpb
GATEWAY_OUT := ../proto/flowmeshpb

# Check if protoc is installed
PROTOC := $(shell command -v protoc 2> /dev/null)
ifndef PROTOC
	$(error "protoc is not installed. Please install Protocol Buffers compiler")
endif

# Check if protoc-gen-go is installed
PROTOC_GEN_GO := $(shell go list -f '{{.Dir}}' -m github.com/golang/protobuf/protoc-gen-go 2> /dev/null)
ifndef PROTOC_GEN_GO
	$(error "protoc-gen-go is not installed. Run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest")
endif

# Check if protoc-gen-go-grpc is installed
PROTOC_GEN_GO_GRPC := $(shell go list -f '{{.Dir}}' -m google.golang.org/grpc/cmd/protoc-gen-go-grpc 2> /dev/null)
ifndef PROTOC_GEN_GO_GRPC
	$(error "protoc-gen-go-grpc is not installed. Run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest")
endif

# Check if protoc-gen-grpc-gateway is installed
PROTOC_GEN_GRPC_GATEWAY := $(shell go list -f '{{.Dir}}' -m github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway 2> /dev/null)
ifndef PROTOC_GEN_GRPC_GATEWAY
	$(error "protoc-gen-grpc-gateway is not installed. Run: go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest")
endif

# Check if protoc-gen-openapiv2 is installed (optional, for OpenAPI docs)
PROTOC_GEN_OPENAPIV2 := $(shell go list -f '{{.Dir}}' -m github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 2> /dev/null)

proto: generate

generate:
	@echo "Generating Go code from $(PROTO_FILE)..."
	@mkdir -p $(GO_OUT)
	protoc \
		-I=$(PROTO_DIR) \
		-I=$$(go list -f '{{.Dir}}' -m github.com/grpc-ecosystem/grpc-gateway/v2)/third_party/googleapis \
		--go_out=$(GO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=$(GATEWAY_OUT) \
		--grpc-gateway_opt=paths=source_relative \
		--grpc-gateway_opt=logtostderr=true \
		$(PROTO_FILE)
	@echo "Generated code in $(GO_OUT)"

clean:
	@echo "Cleaning generated files..."
	@rm -rf $(GO_OUT)
	@echo "Cleaned"

