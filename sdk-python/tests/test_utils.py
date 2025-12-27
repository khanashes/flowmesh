"""
Test utility functions
"""

from datetime import datetime
from flowmesh.utils import build_resource_path, timestamp_to_nanos, nanos_to_timestamp


def test_build_resource_path():
    """Test resource path building"""
    path = build_resource_path("tenant", "namespace", "stream", "name")
    assert path["tenant"] == "tenant"
    assert path["namespace"] == "namespace"
    assert path["resource_type"] == "stream"
    assert path["name"] == "name"


def test_timestamp_conversion():
    """Test timestamp to nanos conversion"""
    dt = datetime.now()
    nanos = timestamp_to_nanos(dt)
    assert isinstance(nanos, int)
    assert nanos > 0

    # Round-trip conversion
    dt2 = nanos_to_timestamp(nanos)
    assert abs((dt2 - dt).total_seconds()) < 1.0  # Within 1 second


def test_nanos_to_timestamp():
    """Test nanos to timestamp conversion"""
    # Test with known timestamp (2024-01-01 00:00:00 UTC)
    nanos = 1704067200000000000
    dt = nanos_to_timestamp(nanos)
    assert isinstance(dt, datetime)
    assert dt.year == 2024
    assert dt.month == 1
    assert dt.day == 1

