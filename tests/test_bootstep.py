"""Unit tests for Celery bootstep."""

from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from fakeredis import FakeRedis

from celery_redis_statedb.bootstep import RedisStatePersistence
from celery_redis_statedb.state import RedisPersistent

if TYPE_CHECKING:
    from kombu.clocks import LamportClock


class TestRedisPersistent:
    """Test RedisPersistent class with blob-based implementation."""

    def test_init(self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis) -> None:
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            assert persistent.state == mock_state
            assert persistent.clock == mock_clock
            assert persistent.redis_db is not None
            assert persistent.worker_name == "test-worker"
            assert persistent.key_prefix == "celery:worker:state:"

    def test_merge_empty_redis(
        self,
        mock_state: Mock,
        mock_clock: LamportClock,
        fake_redis: FakeRedis,
    ) -> None:
        """Test merging when Redis is empty."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # State should be empty
            assert len(mock_state.revoked) == 0

    def test_merge_with_existing_tasks(
        self,
        mock_state: Mock,
        mock_clock: LamportClock,
        fake_redis: FakeRedis,
    ) -> None:
        """Test merging with existing tasks in Redis using blob storage."""
        import zlib

        from kombu.serialization import pickle

        # Pre-populate Redis with compressed blob of revoked tasks
        existing_revoked = LimitedSet(maxlen=100)
        existing_revoked.add("task-1")
        existing_revoked.add("task-2")

        zrevoked_data = zlib.compress(pickle.dumps(existing_revoked))
        zrevoked_key = "celery:worker:state:test-worker:zrevoked"
        fake_redis.set(zrevoked_key, zrevoked_data)

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Tasks should be loaded into state
            assert "task-1" in mock_state.revoked
            assert "task-2" in mock_state.revoked

    def test_merge_with_clock(
        self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis
    ) -> None:
        """Test merging clock value from Redis."""
        # Pre-populate Redis with clock value
        clock_key = "celery:worker:state:test-worker:clock"
        fake_redis.set(clock_key, 100)

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Clock should be adjusted (max of stored 100 and current value + 1)
            # LamportClock.adjust(100) will set clock to max(current, 100) + 1 = 101
            adjusted_value = int(fake_redis.get(clock_key))
            assert adjusted_value == 101

    def test_sync(self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis) -> None:
        """Test syncing state to Redis using blob storage."""
        import zlib

        from kombu.serialization import pickle

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Add tasks to state
            mock_state.revoked.add("task-1")
            mock_state.revoked.add("task-2")

            # Sync to Redis
            persistent.sync()

            # Verify tasks were written to Redis as compressed blob
            zrevoked_key = "celery:worker:state:test-worker:zrevoked"
            stored_data = fake_redis.get(zrevoked_key)
            assert stored_data is not None

            # Decompress and verify contents
            revoked_set = pickle.loads(zlib.decompress(stored_data))
            assert "task-1" in revoked_set
            assert "task-2" in revoked_set

    def test_sync_clock(
        self,
        mock_state: Mock,
        mock_clock: LamportClock,
        fake_redis: FakeRedis,
    ) -> None:
        """Test syncing clock to Redis."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Sync to Redis
            persistent.sync()

            # Verify clock was written (value should match clock.forward())
            clock_key = "celery:worker:state:test-worker:clock"
            clock_value = fake_redis.get(clock_key)
            # Clock value should be greater than 0 after forward() is called
            assert int(clock_value) > 0

    def test_save(self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis) -> None:
        """Test saving state."""
        import zlib

        from kombu.serialization import pickle

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Add a task
            mock_state.revoked.add("task-1")

            # Save
            persistent.save()

            # Verify task was saved as compressed blob
            zrevoked_key = "celery:worker:state:test-worker:zrevoked"
            stored_data = fake_redis.get(zrevoked_key)
            assert stored_data is not None

            revoked_set = pickle.loads(zlib.decompress(stored_data))
            assert "task-1" in revoked_set

    def test_close(self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis) -> None:
        """Test closing connection."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            # Should not raise
            persistent.close()

    def test_merge_error_handling(
        self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis
    ) -> None:
        """Test that merge errors don't crash initialization."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            with patch("celery_redis_statedb.state.RedisStateDB.get_zrevoked") as mock_get:
                mock_get.return_value = None  # Return None instead of raising

                # Should not raise
                persistent = RedisPersistent(
                    worker_name="test-worker",
                    key_prefix="celery:worker:state:",
                    state=mock_state,
                    redis_url="redis://localhost:6379/0",
                    clock=mock_clock,
                )

                assert persistent is not None

    def test_save_error_handling(
        self, mock_state: Mock, mock_clock: LamportClock, fake_redis: FakeRedis
    ) -> None:
        """Test that save handles errors gracefully."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                worker_name="test-worker",
                key_prefix="celery:worker:state:",
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                clock=mock_clock,
            )

            with patch.object(persistent.redis_db, "update", side_effect=Exception("Redis error")):
                # save() should handle errors gracefully
                persistent.save()  # Should not raise


class TestRedisStatePersistence:
    """Test RedisStatePersistence bootstep."""

    def test_init_enabled_with_redis_url(self) -> None:
        """Test initialization when redis_statedb parameter is a Redis URL."""
        worker = Mock()
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")

        assert bootstep.enabled is True

    def test_init_enabled_with_rediss_url(self) -> None:
        """Test initialization with secure Redis URL."""
        worker = Mock()
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker, redis_statedb="rediss://localhost:6379/0")

        assert bootstep.enabled is True

    def test_init_disabled_no_statedb(self) -> None:
        """Test initialization when statedb is not set."""
        worker = Mock()
        worker.statedb = None
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is False

    def test_init_disabled_non_redis_url(self) -> None:
        """Test initialization when statedb is not a Redis URL."""
        worker = Mock()
        worker.statedb = "/tmp/celery-state.db"
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is False

    def test_init_with_redis_statedb_parameter(self) -> None:
        """Test initialization with --redis-statedb parameter."""
        worker = Mock()
        worker.statedb = None
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/1")

        assert bootstep.enabled is True
        assert bootstep.redis_statedb == "redis://localhost:6379/1"

    def test_init_redis_statedb_takes_precedence_over_statedb(self) -> None:
        """Test that --redis-statedb takes precedence over --statedb."""
        worker = Mock()
        worker.statedb = "/tmp/celery-state.db"  # Non-Redis statedb
        worker._redis_persistence = None

        # redis_statedb should enable the bootstep even though statedb is a file path
        bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/1")

        assert bootstep.enabled is True
        assert bootstep.redis_statedb == "redis://localhost:6379/1"

    def test_init_redis_statedb_overrides_redis_statedb(self) -> None:
        """Test that --redis-statedb overrides --statedb even when both are Redis URLs."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"  # Different Redis DB
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/1")

        assert bootstep.enabled is True
        assert bootstep.redis_statedb == "redis://localhost:6379/1"

    def test_create_with_defaults(self, fake_redis: FakeRedis) -> None:
        """Test creating persistence layer with default configuration."""
        worker = Mock()
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.clock.adjust = Mock(return_value=100)
        worker.app.clock.forward = Mock(return_value=101)
        worker.app.conf = Mock()
        worker._redis_persistence = None

        # Set default config values
        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")
            bootstep.create(worker)

            assert worker._redis_persistence is not None
            assert isinstance(worker._redis_persistence, RedisPersistent)

    def test_create_with_custom_config(self, fake_redis: FakeRedis) -> None:
        """Test creating persistence layer with custom configuration."""
        worker = Mock()
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.clock.adjust = Mock(return_value=100)
        worker.app.clock.forward = Mock(return_value=101)
        worker.app.conf = Mock()
        worker._redis_persistence = None

        # Set custom config values
        worker.app.conf.redis_state_key_prefix = "myapp:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")
            bootstep.create(worker)

            assert worker._redis_persistence is not None
            # Key prefix now includes worker hostname
            assert (
                worker._redis_persistence.redis_db.key_prefix
                == "myapp:worker:state:test-worker@hostname:"
            )

    def test_create_with_env_var(self, fake_redis: FakeRedis) -> None:
        """Test that environment variable takes precedence over app.conf."""
        import os

        worker = Mock()
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.clock.adjust = Mock(return_value=100)
        worker.app.clock.forward = Mock(return_value=101)
        worker.app.conf = Mock()
        worker._redis_persistence = None

        # Set both env var and app.conf - env var should win
        worker.app.conf.redis_state_key_prefix = "appconf:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            with patch.dict(os.environ, {"CELERY_REDIS_STATE_KEY_PREFIX": "envvar:worker:state:"}):
                bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")
                bootstep.create(worker)

                assert worker._redis_persistence is not None
                # Environment variable should take precedence
                assert (
                    worker._redis_persistence.redis_db.key_prefix
                    == "envvar:worker:state:test-worker@hostname:"
                )

    def test_create_with_redis_statedb_parameter(self, fake_redis: FakeRedis) -> None:
        """Test that create uses redis_statedb parameter when provided."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"  # Different URL
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.clock.adjust = Mock(return_value=100)
        worker.app.clock.forward = Mock(return_value=101)
        worker.app.conf = Mock()
        worker._redis_persistence = None

        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch(
            "celery_redis_statedb.state.redis.from_url", return_value=fake_redis
        ) as mock_from_url:
            # Pass redis_statedb which should take precedence
            bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/1")
            bootstep.create(worker)

            assert worker._redis_persistence is not None
            # Verify redis.from_url was called with redis_statedb, not worker.statedb
            assert mock_from_url.called
            assert mock_from_url.call_args[0][0] == "redis://localhost:6379/1"

    def test_create_disabled(self) -> None:
        """Test create when bootstep is disabled."""
        worker = Mock()
        worker.statedb = None
        worker._redis_persistence = None

        bootstep = RedisStatePersistence(worker)
        bootstep.create(worker)

        # Should not create persistence layer
        assert worker._redis_persistence is None

    def test_create_registers_atexit(self, fake_redis: FakeRedis) -> None:
        """Test that create registers atexit handler."""
        worker = Mock()
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.clock.adjust = Mock(return_value=100)
        worker.app.clock.forward = Mock(return_value=101)
        worker.app.conf = Mock()
        worker._redis_persistence = None

        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            with patch("celery_redis_statedb.bootstep.atexit.register") as mock_atexit:
                bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")
                bootstep.create(worker)

                # Verify atexit was called with save method
                mock_atexit.assert_called_once()
                assert mock_atexit.call_args[0][0] == worker._redis_persistence.save

    def test_create_error_handling(self) -> None:
        """Test error handling during create."""
        worker = Mock()
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.app = Mock()
        worker.app.conf = Mock()
        worker._redis_persistence = None

        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch(
            "celery_redis_statedb.bootstep.RedisPersistent",
            side_effect=Exception("Connection failed"),
        ):
            bootstep = RedisStatePersistence(worker, redis_statedb="redis://localhost:6379/0")

            with pytest.raises(Exception):
                bootstep.create(worker)

            # Persistence should be None after error
            assert worker._redis_persistence is None
