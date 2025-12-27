"""
Test exception handling
"""

import pytest
import grpc
from flowmesh.exceptions import (
    FlowMeshError,
    FlowMeshConnectionError,
    FlowMeshAuthError,
    FlowMeshNotFoundError,
    FlowMeshPermissionDeniedError,
    handle_grpc_error,
)


def test_flowmesh_error():
    """Test base FlowMeshError"""
    error = FlowMeshError("Test error", code=1, details="details")
    assert str(error) == "Test error"
    assert error.code == 1
    assert error.details == "details"


def test_grpc_error_conversion():
    """Test gRPC error to FlowMesh exception conversion"""
    # Create a mock gRPC error
    class MockRpcError(Exception):
        def code(self):
            return grpc.StatusCode.NOT_FOUND

        def details(self):
            return "Resource not found"

    error = MockRpcError()
    exception = handle_grpc_error(error, "read")
    
    assert isinstance(exception, FlowMeshNotFoundError)
    assert "read failed" in str(exception)


def test_auth_error():
    """Test authentication error"""
    error = FlowMeshAuthError("Invalid token")
    assert isinstance(error, FlowMeshError)
    assert "token" in str(error).lower()


def test_connection_error():
    """Test connection error"""
    error = FlowMeshConnectionError("Connection failed")
    assert isinstance(error, FlowMeshError)
    assert "connection" in str(error).lower()

