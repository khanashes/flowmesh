"""
Test type definitions
"""

from datetime import datetime
from flowmesh.types import (
    Event,
    Message,
    Job,
    QueueStats,
    RetryPolicy,
    ConsumerGroupState,
)


def test_event():
    """Test Event type"""
    event = Event(payload=b"test", headers={"key": "value"})
    assert event.payload == b"test"
    assert event.headers == {"key": "value"}


def test_message():
    """Test Message type"""
    now = datetime.now()
    message = Message(
        id="msg-123",
        resource_path="tenant/namespace/stream/name",
        partition=0,
        offset=100,
        payload=b"payload",
        headers={"key": "value"},
        created_at=now,
        schema_version=1,
    )
    assert message.id == "msg-123"
    assert message.offset == 100
    assert message.payload == b"payload"


def test_job():
    """Test Job type"""
    now = datetime.now()
    job = Job(
        id="job-123",
        resource_path="tenant/namespace/queue/name",
        seq=50,
        payload=b"job payload",
        headers={"priority": "high"},
        created_at=now,
        visible_at=now,
        reserve_until=now,
        attempts=1,
    )
    assert job.id == "job-123"
    assert job.seq == 50
    assert job.attempts == 1


def test_queue_stats():
    """Test QueueStats type"""
    stats = QueueStats(
        total_jobs=100,
        pending_jobs=10,
        in_flight_jobs=5,
        completed_jobs=80,
        failed_jobs=5,
        oldest_job_age_seconds=3600,
    )
    assert stats.total_jobs == 100
    assert stats.pending_jobs == 10


def test_retry_policy():
    """Test RetryPolicy type"""
    policy = RetryPolicy(
        max_attempts=3,
        initial_backoff_seconds=1,
        max_backoff_seconds=60,
        backoff_multiplier=2.0,
        backoff_strategy="exponential",
    )
    assert policy.max_attempts == 3
    assert policy.backoff_strategy == "exponential"


def test_consumer_group_state():
    """Test ConsumerGroupState type"""
    state = ConsumerGroupState(
        stream="tenant/namespace/stream/name",
        group="consumer-group",
        partition=0,
        committed_offset=100,
        latest_offset=150,
        lag=50,
    )
    assert state.stream == "tenant/namespace/stream/name"
    assert state.lag == 50

