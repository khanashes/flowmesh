#!/bin/bash
# Generate Python gRPC stubs from proto file

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/../engine/api/proto"
OUTPUT_DIR="$SCRIPT_DIR/flowmesh/_generated"

echo "Generating Python gRPC stubs..."
echo "Proto directory: $PROTO_DIR"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Generate Python code
python -m grpc_tools.protoc \
    --proto_path="$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    --mypy_out="$OUTPUT_DIR" \
    "$PROTO_DIR/flowmesh.proto"

echo "✓ Python gRPC stubs generated successfully"
echo "Output: $OUTPUT_DIR"

