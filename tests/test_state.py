"""Unit tests for Redis state database."""

import time
from unittest.mock import MagicMock, patch

import pytest
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from fakeredis import FakeRedis

from celery_redis_statedb.state import RedisStateDB


@pytest.fixture
def fake_redis() -> FakeRedis:
    """Create a fake Redis instance for testing."""
    return FakeRedis(decode_responses=False)


@pytest.fixture
def redis_db(fake_redis: FakeRedis) -> RedisStateDB:
    """Create a RedisStateDB instance with fake Redis."""
    with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
        db = RedisStateDB(
            redis_url="redis://localhost:6379/0",
            worker_name="test-worker",
            key_prefix="test:",
        )
        return db


class TestRedisStateDB:
    """Test RedisStateDB functionality."""

    def test_init(self, redis_db: RedisStateDB) -> None:
        """Test initialization."""
        assert redis_db.worker_name == "test-worker"
        assert redis_db.key_prefix == "test:test-worker:"
        assert redis_db.redis_client is not None

    def test_get_key(self, redis_db: RedisStateDB) -> None:
        """Test key prefix generation."""
        assert redis_db._get_key("revoked") == "test:test-worker:revoked"
        assert redis_db._get_key("clock") == "test:test-worker:clock"

    def test_add_revoked(self, redis_db: RedisStateDB) -> None:
        """Test adding a revoked task."""
        task_id = "task-123"
        timestamp = time.time()

        redis_db.add_revoked(task_id, timestamp)

        # Verify task was added
        assert redis_db.is_revoked(task_id)
        assert task_id in redis_db.get_revoked()

    def test_add_revoked_without_timestamp(self, redis_db: RedisStateDB) -> None:
        """Test adding a revoked task without explicit timestamp."""
        task_id = "task-456"

        redis_db.add_revoked(task_id)

        assert redis_db.is_revoked(task_id)

    def test_add_revoked_bulk(self, redis_db: RedisStateDB) -> None:
        """Test bulk adding revoked tasks."""
        # Create a LimitedSet with tasks
        revoked_set = LimitedSet(maxlen=100)
        time.time()

        tasks = [f"task-{i}" for i in range(10)]
        for task_id in tasks:
            revoked_set.add(task_id)

        redis_db.add_revoked_bulk(revoked_set)

        # Verify all tasks were added
        revoked = redis_db.get_revoked()
        for task_id in tasks:
            assert task_id in revoked

    def test_is_revoked(self, redis_db: RedisStateDB) -> None:
        """Test checking if task is revoked."""
        task_id = "task-789"

        # Task should not be revoked initially
        assert not redis_db.is_revoked(task_id)

        # Add task
        redis_db.add_revoked(task_id)

        # Task should now be revoked
        assert redis_db.is_revoked(task_id)

    def test_get_revoked(self, redis_db: RedisStateDB) -> None:
        """Test getting all revoked tasks."""
        tasks = [f"task-{i}" for i in range(5)]

        # Add multiple tasks
        for task_id in tasks:
            redis_db.add_revoked(task_id)

        # Get all revoked tasks
        revoked = redis_db.get_revoked()

        # Verify all tasks are present
        assert len(revoked) == 5
        for task_id in tasks:
            assert task_id in revoked

    def test_purge_old_revoked(self, redis_db: RedisStateDB) -> None:
        """Test purging old revoked tasks."""
        current_time = time.time()
        old_time = current_time - 7200  # 2 hours ago

        # Add old task
        redis_db.add_revoked("old-task", old_time)

        # Add recent task
        redis_db.add_revoked("recent-task", current_time)

        # Purge tasks older than 1 hour (3600 seconds)
        count = redis_db.purge_old_revoked(older_than_seconds=3600)

        # Old task should be removed
        assert count == 1
        assert not redis_db.is_revoked("old-task")
        # Recent task should still be there
        assert redis_db.is_revoked("recent-task")

    def test_clear_revoked(self, redis_db: RedisStateDB) -> None:
        """Test clearing all revoked tasks."""
        # Add some tasks
        for i in range(5):
            redis_db.add_revoked(f"task-{i}")

        # Verify tasks exist
        assert len(redis_db.get_revoked()) == 5

        # Clear all tasks
        redis_db.clear_revoked()

        # Verify all tasks are gone
        assert len(redis_db.get_revoked()) == 0

    def test_clock_operations(self, redis_db: RedisStateDB) -> None:
        """Test clock get/set/increment operations."""
        # Initial clock should be 0
        assert redis_db.get_clock() == 0

        # Set clock value
        redis_db.set_clock(42)
        assert redis_db.get_clock() == 42

        # Increment clock
        new_value = redis_db.increment_clock()
        assert new_value == 43
        assert redis_db.get_clock() == 43

    def test_ping(self, redis_db: RedisStateDB) -> None:
        """Test Redis connection ping."""
        assert redis_db.ping() is True

    def test_close(self, redis_db: RedisStateDB) -> None:
        """Test closing Redis connection."""
        # Should not raise an exception
        redis_db.close()

    def test_retry_operation_success(self, redis_db: RedisStateDB) -> None:
        """Test successful retry operation."""
        mock_operation = MagicMock(return_value="success")

        result = redis_db._retry_operation(mock_operation, "arg1", key="value")

        assert result == "success"
        assert mock_operation.call_count == 1

    def test_retry_operation_with_retries(self, redis_db: RedisStateDB) -> None:
        """Test retry operation with temporary failures."""
        import redis as redis_module

        # Mock operation that fails twice then succeeds
        mock_operation = MagicMock(
            side_effect=[
                redis_module.ConnectionError("Connection failed"),
                redis_module.ConnectionError("Connection failed"),
                "success",
            ]
        )

        redis_db.retry_delay = 0.001  # Speed up test

        result = redis_db._retry_operation(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 3

    def test_retry_operation_max_retries_exceeded(self, redis_db: RedisStateDB) -> None:
        """Test retry operation when max retries is exceeded."""
        import redis as redis_module

        # Mock operation that always fails
        mock_operation = MagicMock(
            side_effect=redis_module.ConnectionError("Connection failed")
        )

        redis_db.retry_delay = 0.001  # Speed up test

        with pytest.raises(redis_module.ConnectionError):
            redis_db._retry_operation(mock_operation)

        assert mock_operation.call_count == redis_db.max_retries

    def test_concurrent_add_revoked(self, redis_db: RedisStateDB) -> None:
        """Test concurrent additions don't cause issues."""
        import threading

        task_ids = [f"task-{i}" for i in range(100)]
        threads = []

        def add_task(task_id: str) -> None:
            redis_db.add_revoked(task_id)

        # Create threads to add tasks concurrently
        for task_id in task_ids:
            thread = threading.Thread(target=add_task, args=(task_id,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all tasks were added
        revoked = redis_db.get_revoked()
        assert len(revoked) == 100

    def test_empty_bulk_add(self, redis_db: RedisStateDB) -> None:
        """Test bulk add with empty set."""
        empty_set = LimitedSet(maxlen=100)

        # Should not raise an exception
        redis_db.add_revoked_bulk(empty_set)

        assert len(redis_db.get_revoked()) == 0


class TestRedisStateDBErrors:
    """Test error handling in RedisStateDB."""

    def test_is_revoked_error_handling(self, redis_db: RedisStateDB) -> None:
        """Test is_revoked error handling returns False on error."""
        import redis as redis_module

        # Use RedisError so it gets caught by retry logic
        with patch.object(
            redis_db, "_retry_operation", side_effect=redis_module.RedisError("Redis error")
        ):
            # Should return False on error, not raise
            assert redis_db.is_revoked("task-123") is False

    def test_get_revoked_error_handling(self, redis_db: RedisStateDB) -> None:
        """Test get_revoked error handling returns empty set on error."""
        import redis as redis_module

        with patch.object(
            redis_db, "_retry_operation", side_effect=redis_module.RedisError("Redis error")
        ):
            # Should return empty set on error, not raise
            result = redis_db.get_revoked()
            assert result == set()

    def test_purge_old_revoked_error_handling(self, redis_db: RedisStateDB) -> None:
        """Test purge_old_revoked error handling returns 0 on error."""
        import redis as redis_module

        with patch.object(
            redis_db, "_retry_operation", side_effect=redis_module.RedisError("Redis error")
        ):
            # Should return 0 on error, not raise
            result = redis_db.purge_old_revoked(older_than_seconds=3600)
            assert result == 0

    def test_get_clock_error_handling(self, redis_db: RedisStateDB) -> None:
        """Test get_clock error handling returns 0 on error."""
        import redis as redis_module

        with patch.object(
            redis_db, "_retry_operation", side_effect=redis_module.RedisError("Redis error")
        ):
            # Should return 0 on error, not raise
            result = redis_db.get_clock()
            assert result == 0
