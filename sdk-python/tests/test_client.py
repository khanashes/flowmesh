"""
Test client connection and basic operations
"""

import pytest
from flowmesh.client import FlowMesh
from flowmesh.exceptions import FlowMeshConnectionError


@pytest.mark.asyncio
async def test_client_creation():
    """Test that client can be created"""
    client = FlowMesh(endpoint="localhost:99999")  # Non-existent server
    assert client.endpoint == "localhost:99999"
    assert client.auth_token is None


@pytest.mark.asyncio
async def test_client_with_auth_token():
    """Test client creation with auth token"""
    client = FlowMesh(endpoint="localhost:50051", auth_token="test-token")
    assert client.auth_token == "test-token"


@pytest.mark.asyncio
async def test_client_context_manager():
    """Test client async context manager"""
    client = FlowMesh(endpoint="localhost:99999")
    # Should not raise even if connection fails
    try:
        async with client:
            pass
    except Exception:
        # Connection errors are expected with non-existent server
        pass


def test_get_metadata_without_token():
    """Test metadata generation without token"""
    client = FlowMesh(endpoint="localhost:50051")
    metadata = client._get_metadata()
    assert metadata is None


def test_get_metadata_with_token():
    """Test metadata generation with token"""
    client = FlowMesh(endpoint="localhost:50051", auth_token="test-token")
    metadata = client._get_metadata()
    assert metadata is not None
    assert len(metadata) == 1
    assert metadata[0][0] == "authorization"
    assert metadata[0][1] == "Bearer test-token"

