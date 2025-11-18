"""Unit tests for Redis state database with simplified blob-based implementation."""

import zlib
from unittest.mock import patch

from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from kombu.serialization import pickle

from celery_redis_statedb.state import RedisStateDB


class TestRedisStateDB:
    """Test RedisStateDB functionality with blob-based storage."""

    def test_init(self, redis_db: RedisStateDB) -> None:
        """Test initialization."""
        assert redis_db.worker_name == "test-worker"
        assert redis_db.key_prefix == "test:test-worker:"
        assert redis_db.redis_client is not None

    def test_get_key(self, redis_db: RedisStateDB) -> None:
        """Test key prefix generation."""
        assert redis_db._get_key("zrevoked") == "test:test-worker:zrevoked"
        assert redis_db._get_key("clock") == "test:test-worker:clock"

    def test_update_blob(self, redis_db: RedisStateDB) -> None:
        """Test updating state using blob storage."""
        # Create a revoked tasks set
        revoked_set = LimitedSet(maxlen=100)
        revoked_set.add("task-1")
        revoked_set.add("task-2")
        revoked_set.add("task-3")

        # Update to Redis
        success = redis_db.update(zrevoked=revoked_set, clock=42)
        assert success is True

        # Verify data was written
        zrevoked_key = redis_db._get_key("zrevoked")
        clock_key = redis_db._get_key("clock")

        stored_data = redis_db.redis_client.get(zrevoked_key)
        assert stored_data is not None

        # Decompress and verify revoked set
        restored_set = pickle.loads(zlib.decompress(stored_data))
        assert "task-1" in restored_set
        assert "task-2" in restored_set
        assert "task-3" in restored_set

        # Verify clock
        stored_clock = redis_db.redis_client.get(clock_key)
        assert int(stored_clock) == 42

    def test_get_zrevoked_empty(self, redis_db: RedisStateDB) -> None:
        """Test getting revoked tasks when Redis is empty."""
        result = redis_db.get_zrevoked()
        assert result is None

    def test_get_zrevoked_with_data(self, redis_db: RedisStateDB) -> None:
        """Test getting revoked tasks from Redis."""
        # Pre-populate Redis with blob
        revoked_set = LimitedSet(maxlen=100)
        revoked_set.add("task-1")
        revoked_set.add("task-2")

        zrevoked_data = zlib.compress(pickle.dumps(revoked_set))
        zrevoked_key = redis_db._get_key("zrevoked")
        redis_db.redis_client.set(zrevoked_key, zrevoked_data)

        # Get from Redis
        result = redis_db.get_zrevoked()
        assert result is not None
        assert "task-1" in result
        assert "task-2" in result

    def test_get_clock_empty(self, redis_db: RedisStateDB) -> None:
        """Test getting clock when Redis is empty."""
        result = redis_db.get_clock()
        assert result == 0

    def test_get_clock_with_value(self, redis_db: RedisStateDB) -> None:
        """Test getting clock from Redis."""
        clock_key = redis_db._get_key("clock")
        redis_db.redis_client.set(clock_key, 42)

        result = redis_db.get_clock()
        assert result == 42

    def test_set_clock(self, redis_db: RedisStateDB) -> None:
        """Test setting clock value."""
        redis_db.set_clock(123)

        clock_key = redis_db._get_key("clock")
        stored_value = redis_db.redis_client.get(clock_key)
        assert int(stored_value) == 123

    def test_ping(self, redis_db: RedisStateDB) -> None:
        """Test Redis connection ping."""
        assert redis_db.ping() is True

    def test_close(self, redis_db: RedisStateDB) -> None:
        """Test closing Redis connection."""
        # Should not raise an exception
        redis_db.close()

    def test_update_with_empty_set(self, redis_db: RedisStateDB) -> None:
        """Test update with empty revoked set."""
        empty_set = LimitedSet(maxlen=100)

        success = redis_db.update(zrevoked=empty_set, clock=0)
        assert success is True

        # Verify empty set was written
        result = redis_db.get_zrevoked()
        assert result is not None
        assert len(result) == 0

    def test_multiple_updates(self, redis_db: RedisStateDB) -> None:
        """Test multiple sequential updates."""
        # First update
        set1 = LimitedSet(maxlen=100)
        set1.add("task-1")
        redis_db.update(zrevoked=set1, clock=1)

        # Second update overwrites first
        set2 = LimitedSet(maxlen=100)
        set2.add("task-2")
        set2.add("task-3")
        redis_db.update(zrevoked=set2, clock=2)

        # Verify second update is stored
        result = redis_db.get_zrevoked()
        assert result is not None
        assert "task-2" in result
        assert "task-3" in result
        assert redis_db.get_clock() == 2


class TestRedisStateDBErrors:
    """Test error handling in RedisStateDB."""

    def test_get_zrevoked_redis_error(self, redis_db: RedisStateDB) -> None:
        """Test get_zrevoked returns None on Redis error."""
        import redis as redis_module

        with patch.object(
            redis_db.redis_client, "get", side_effect=redis_module.RedisError("Redis error")
        ):
            result = redis_db.get_zrevoked()
            assert result is None

    def test_get_clock_redis_error(self, redis_db: RedisStateDB) -> None:
        """Test get_clock returns 0 on Redis error."""
        import redis as redis_module

        with patch.object(
            redis_db.redis_client, "get", side_effect=redis_module.RedisError("Redis error")
        ):
            result = redis_db.get_clock()
            assert result == 0

    def test_set_clock_redis_error(self, redis_db: RedisStateDB) -> None:
        """Test set_clock handles Redis errors gracefully."""
        import redis as redis_module

        with patch.object(
            redis_db.redis_client, "set", side_effect=redis_module.RedisError("Redis error")
        ):
            # Should not raise, just log error
            redis_db.set_clock(42)

    def test_update_redis_error(self, redis_db: RedisStateDB) -> None:
        """Test update returns False on Redis error."""
        import redis as redis_module

        with patch.object(
            redis_db.redis_client, "pipeline", side_effect=redis_module.RedisError("Redis error")
        ):
            revoked_set = LimitedSet(maxlen=100)
            success = redis_db.update(zrevoked=revoked_set, clock=0)
            assert success is False

    def test_ping_redis_error(self, redis_db: RedisStateDB) -> None:
        """Test ping returns False on Redis error."""
        import redis as redis_module

        with patch.object(
            redis_db.redis_client, "ping", side_effect=redis_module.RedisError("Redis error")
        ):
            result = redis_db.ping()
            assert result is False

    def test_close_error_handling(self, redis_db: RedisStateDB) -> None:
        """Test close handles errors gracefully."""
        with patch.object(redis_db.redis_client, "close", side_effect=Exception("Close error")):
            # Should not raise, just log error
            redis_db.close()

    def test_get_zrevoked_corrupted_data(self, redis_db: RedisStateDB) -> None:
        """Test get_zrevoked handles corrupted data gracefully."""
        # Store corrupted data
        zrevoked_key = redis_db._get_key("zrevoked")
        redis_db.redis_client.set(zrevoked_key, b"corrupted data")

        # Should return None on decompression/unpickling error
        result = redis_db.get_zrevoked()
        assert result is None
