"""Common pytest fixtures for celery-redis-statedb tests."""

from unittest.mock import Mock, patch

import pytest
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from fakeredis import FakeRedis
from kombu.clocks import LamportClock  # type: ignore[import-untyped]

from celery_redis_statedb.state import RedisStateDB


@pytest.fixture
def fake_redis():
    """Create a fake Redis instance for testing."""
    return FakeRedis(decode_responses=False)


@pytest.fixture
def mock_state():
    """Create a mock worker state."""
    state = Mock()
    state.revoked = LimitedSet(maxlen=100)
    return state


@pytest.fixture
def mock_clock():
    """Create a Celery logical clock"""
    return LamportClock()


@pytest.fixture
def redis_db(fake_redis):
    """Create a RedisStateDB instance with fake Redis."""
    with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
        db = RedisStateDB(
            redis_url="redis://localhost:6379/0",
            worker_name="test-worker",
            key_prefix="test:",
        )
        return db


@pytest.fixture
def mock_worker():
    """Create a mock Celery worker with real LamportClock."""
    worker = Mock()
    worker.statedb = "redis://localhost:6379/0"
    worker.hostname = "test-worker@hostname"
    worker.state = Mock()
    worker.state.revoked = LimitedSet(maxlen=100)
    worker.app = Mock()
    worker.app.clock = LamportClock()
    worker.app.conf = Mock()
    worker._persistence = None
    return worker


@pytest.fixture
def mock_celery_app():
    """Create a mock Celery app."""
    app = Mock()
    app.main = "testapp"
    app.steps = {"worker": set()}
    return app
