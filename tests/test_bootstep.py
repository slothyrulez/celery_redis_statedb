"""Unit tests for Celery bootstep."""

from unittest.mock import Mock, patch

import pytest
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from fakeredis import FakeRedis

from celery_redis_statedb.bootstep import RedisPersistent, RedisStatePersistence


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock worker state."""
    state = Mock()
    state.revoked = LimitedSet(maxlen=100)
    return state


@pytest.fixture
def mock_clock() -> Mock:
    """Create a mock logical clock."""
    clock = Mock()
    clock.adjust = Mock()
    clock.forward = Mock(return_value=42)
    return clock


@pytest.fixture
def fake_redis() -> FakeRedis:
    """Create a fake Redis instance for testing."""
    return FakeRedis(decode_responses=False)


class TestRedisPersistent:
    """Test RedisPersistent class."""

    def test_init(self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis) -> None:
        """Test initialization."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
                key_prefix="test:",
            )

            assert persistent.state == mock_state
            assert persistent.clock == mock_clock
            assert persistent.redis_db is not None

    def test_merge_empty_redis(
        self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis
    ) -> None:
        """Test merging when Redis is empty."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # State should be empty
            assert len(mock_state.revoked) == 0

    def test_merge_with_existing_tasks(
        self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis
    ) -> None:
        """Test merging with existing tasks in Redis."""
        import time

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            # Pre-populate Redis with tasks (with worker name in key)
            key = "celery:worker:state:test-worker:revoked"
            timestamp = time.time()
            fake_redis.zadd(key, {"task-1": timestamp, "task-2": timestamp})

            RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Tasks should be loaded into state
            assert "task-1" in mock_state.revoked
            assert "task-2" in mock_state.revoked

    def test_merge_with_clock(
        self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis
    ) -> None:
        """Test merging clock value from Redis."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            # Pre-populate Redis with clock value (with worker name in key)
            clock_key = "celery:worker:state:test-worker:clock"
            fake_redis.set(clock_key, 100)

            RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Clock should be adjusted
            mock_clock.adjust.assert_called_with(100)

    def test_sync(self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis) -> None:
        """Test syncing state to Redis."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Add tasks to state
            mock_state.revoked.add("task-1")
            mock_state.revoked.add("task-2")

            # Sync to Redis
            persistent.sync()

            # Verify tasks were written to Redis (with worker name in key)
            key = "celery:worker:state:test-worker:revoked"
            members = fake_redis.zrange(key, 0, -1)
            task_ids = {m.decode("utf-8") if isinstance(m, bytes) else m for m in members}

            assert "task-1" in task_ids
            assert "task-2" in task_ids

    def test_sync_clock(self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis) -> None:
        """Test syncing clock to Redis."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Sync to Redis
            persistent.sync()

            # Verify clock was written (with worker name in key)
            clock_key = "celery:worker:state:test-worker:clock"
            clock_value = fake_redis.get(clock_key)
            assert int(clock_value) == 42

    def test_save(self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis) -> None:
        """Test saving state."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Add a task
            mock_state.revoked.add("task-1")

            # Save
            persistent.save()

            # Verify task was saved (with worker name in key)
            key = "celery:worker:state:test-worker:revoked"
            members = fake_redis.zrange(key, 0, -1)
            task_ids = {m.decode("utf-8") if isinstance(m, bytes) else m for m in members}
            assert "task-1" in task_ids

    def test_close(self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis) -> None:
        """Test closing connection."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            # Should not raise
            persistent.close()

    def test_merge_error_handling(
        self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis
    ) -> None:
        """Test that merge errors don't crash initialization."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            with patch("celery_redis_statedb.state.RedisStateDB.get_revoked") as mock_get:
                mock_get.side_effect = Exception("Redis error")

                # Should not raise
                persistent = RedisPersistent(
                    state=mock_state,
                    redis_url="redis://localhost:6379/0",
                    worker_name="test-worker",
                    clock=mock_clock,
                )

                assert persistent is not None

    def test_sync_error_handling(
        self, mock_state: Mock, mock_clock: Mock, fake_redis: FakeRedis
    ) -> None:
        """Test that sync errors don't crash the worker."""
        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            persistent = RedisPersistent(
                state=mock_state,
                redis_url="redis://localhost:6379/0",
                worker_name="test-worker",
                clock=mock_clock,
            )

            with patch.object(
                persistent.redis_db, "add_revoked_bulk", side_effect=Exception("Redis error")
            ):
                # Should not raise
                persistent.sync()


class TestRedisStatePersistence:
    """Test RedisStatePersistence bootstep."""

    def test_init_enabled_with_redis_url(self) -> None:
        """Test initialization when statedb is a Redis URL."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"
        worker._persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is True

    def test_init_enabled_with_rediss_url(self) -> None:
        """Test initialization with secure Redis URL."""
        worker = Mock()
        worker.statedb = "rediss://localhost:6379/0"
        worker._persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is True

    def test_init_disabled_no_statedb(self) -> None:
        """Test initialization when statedb is not set."""
        worker = Mock()
        worker.statedb = None
        worker._persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is False

    def test_init_disabled_non_redis_url(self) -> None:
        """Test initialization when statedb is not a Redis URL."""
        worker = Mock()
        worker.statedb = "/tmp/celery-state.db"
        worker._persistence = None

        bootstep = RedisStatePersistence(worker)

        assert bootstep.enabled is False

    def test_create_with_defaults(self, fake_redis: FakeRedis) -> None:
        """Test creating persistence layer with default configuration."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.conf = Mock()
        worker._persistence = None

        # Set default config values
        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            bootstep = RedisStatePersistence(worker)
            bootstep.create(worker)

            assert worker._persistence is not None
            assert isinstance(worker._persistence, RedisPersistent)

    def test_create_with_custom_config(self, fake_redis: FakeRedis) -> None:
        """Test creating persistence layer with custom configuration."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.conf = Mock()
        worker._persistence = None

        # Set custom config values
        worker.app.conf.redis_state_key_prefix = "myapp:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            bootstep = RedisStatePersistence(worker)
            bootstep.create(worker)

            assert worker._persistence is not None
            # Key prefix now includes worker hostname
            assert (
                worker._persistence.redis_db.key_prefix
                == "myapp:worker:state:test-worker@hostname:"
            )

    def test_create_disabled(self) -> None:
        """Test create when bootstep is disabled."""
        worker = Mock()
        worker.statedb = None
        worker._persistence = None

        bootstep = RedisStatePersistence(worker)
        bootstep.create(worker)

        # Should not create persistence layer
        assert worker._persistence is None

    def test_create_registers_atexit(self, fake_redis: FakeRedis) -> None:
        """Test that create registers atexit handler."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.state.revoked = LimitedSet(maxlen=100)
        worker.app = Mock()
        worker.app.clock = Mock()
        worker.app.conf = Mock()
        worker._persistence = None

        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch("celery_redis_statedb.state.redis.from_url", return_value=fake_redis):
            with patch("celery_redis_statedb.bootstep.atexit.register") as mock_atexit:
                bootstep = RedisStatePersistence(worker)
                bootstep.create(worker)

                # Verify atexit was called with save method
                mock_atexit.assert_called_once()
                assert mock_atexit.call_args[0][0] == worker._persistence.save

    def test_create_error_handling(self) -> None:
        """Test error handling during create."""
        worker = Mock()
        worker.statedb = "redis://localhost:6379/0"
        worker.hostname = "test-worker@hostname"
        worker.state = Mock()
        worker.app = Mock()
        worker.app.conf = Mock()
        worker._persistence = None

        worker.app.conf.redis_state_key_prefix = "celery:worker:state:"

        with patch(
            "celery_redis_statedb.bootstep.RedisPersistent",
            side_effect=Exception("Connection failed"),
        ):
            bootstep = RedisStatePersistence(worker)

            with pytest.raises(Exception):
                bootstep.create(worker)

            # Persistence should be None after error
            assert worker._persistence is None
