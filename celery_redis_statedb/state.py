import logging
import zlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import redis
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from kombu.serialization import pickle, pickle_protocol  # type: ignore[attr-defined]

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class RedisStateDB:
    """Redis-based state database with per-worker key isolation.

    Each worker maintains its own state using a unique key prefix based on
    the worker name/hostname. This eliminates concurrency issues since workers
    never write to the same keys.

    Revoked tasks persist indefinitely in Redis (matching Celery's default behavior).
    Use clear_revoked() to manually clean up if needed.

    Attributes:
        redis_client: Redis client instance
        key_prefix: Prefix for all Redis keys (includes worker identifier)
        max_retries: Maximum number of retries for Redis operations
        retry_delay: Delay between retries in seconds
    """

    protocol: int = pickle_protocol
    compress: Callable[[bytes], bytes] = zlib.compress
    decompress: Callable[[bytes], bytes] = zlib.decompress

    def __init__(
        self,
        redis_url: str,
        worker_name: str,
        key_prefix: str = "celery:worker:state:",
        max_retries: int = 3,
        retry_delay: float = 0.1,
    ) -> None:
        """Initialize Redis state database.

        Args:
            redis_url: Redis connection URL (e.g., 'redis://localhost:6379/0')
            worker_name: Unique worker identifier (hostname or worker name)
            key_prefix: Base prefix for all Redis keys
            max_retries: Maximum number of retries for Redis operations
            retry_delay: Delay between retries in seconds

        Raises:
            ImproperlyConfigured: If redis library is not installed
        """
        self.redis_url = redis_url
        self.worker_name = worker_name
        # Include worker name in key prefix for isolation
        self.key_prefix = f"{key_prefix}{worker_name}:"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.redis_client = redis.from_url(
            redis_url,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
        )

        logger.info(
            "[redis-statedb] RedisStateDB initialized for worker=%s, prefix=%s",
            self.worker_name,
            self.key_prefix,
        )

    def _get_key(self, name: str) -> str:
        return f"{self.key_prefix}{name}"

    def update(self, zrevoked: LimitedSet, clock: Any) -> bool:
        zrevoked_key = self._get_key("zrevoked")
        zrevoked_data = self.compress(self._dumps(zrevoked))
        clock_key = self._get_key("clock")
        clock_data = clock
        _success = False
        try:
            pipe = self.redis_client.pipeline()
            pipe.set(zrevoked_key, zrevoked_data)
            pipe.set(clock_key, clock_data)
            pipe.execute()
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
            logger.error("[redis-statedb] Error syncing state to Redis: %s", exc)
        else:
            _success = True
            logger.debug("[redis-statedb] Worker state synced to Redis successfully")
        return _success

    def _dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=self.protocol)

    def get_zrevoked(self) -> LimitedSet | None:
        zrevoked_key = self._get_key("zrevoked")

        try:
            value = self.redis_client.get(zrevoked_key)
            if value is None:
                return None
            data = pickle.loads(self.decompress(value))
            logger.debug("[redis-statedb] Revoked tasks retrieved successfully")
            return data
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
            logger.error("[redis-statedb] Failed to get revoked tasks: %s", exc)
            return None
        except Exception as exc:
            logger.error(
                "[redis-statedb] Failed to deserialize revoked tasks (corrupted data?): %s", exc
            )
            return None

    def get_clock(self) -> int:
        clock_key = self._get_key("clock")

        try:
            value = self.redis_client.get(clock_key)
            return int(value) if value else 0
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
            logger.error("[redis-statedb] Failed to get clock value: %s", exc)
            return 0

    def set_clock(self, value: int) -> None:
        clock_key = self._get_key("clock")

        try:
            self.redis_client.set(clock_key, value)
            logger.debug("[redis-statedb] Set clock value to %d", value)
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
            logger.error("[redis-statedb] Failed to set clock value: %s", exc)

    def close(self) -> None:
        try:
            self.redis_client.close()
            logger.info("[redis-statedb] Closed Redis connection")
        except Exception as exc:
            logger.error("[redis-statedb] Error closing Redis connection: %s", exc)

    def ping(self) -> bool:
        try:
            result: bool = self.redis_client.ping()
            return result
        except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
            logger.error("[redis-statedb] Error testing redis connection: %s", exc)
            return False


class RedisPersistent:
    """Redis-based persistent state manager for Celery workers."""

    def __init__(
        self,
        worker_name: str,
        key_prefix: str,
        state: Any,
        redis_url: str,
        clock: Any | None = None,
    ) -> None:
        """Initialize Redis persistent state.

        Args:
            state: Worker state object
            redis_url: Redis connection URL
            clock: Optional logical clock

        Raises:
            AttributeError: If state._worker_name is not set
        """
        self.state = state
        self.clock = clock
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.worker_name = worker_name

        self.redis_db = RedisStateDB(
            redis_url=self.redis_url,
            worker_name=self.worker_name,
            key_prefix=self.key_prefix,
        )

        logger.info(
            "[redis-statedb] Initializing persistent state for worker=%s from %s key_prefix=%s",
            self.worker_name,
            self.redis_url,
            self.key_prefix,
        )
        # Load existing state from Redis
        self.merge()

    def merge(self) -> None:
        """Merge existing Redis state into worker state."""
        self._merge_with(self.db)

    def _merge_with(self, db: RedisStateDB) -> None:
        self._merge_revoked(db)
        self._merge_clock(db)

    def _merge_revoked(self, db: RedisStateDB) -> None:
        if zrevoked := db.get_zrevoked():
            self._revoked_tasks.update(zrevoked)
        # purge expired items at boot
        self._revoked_tasks.purge()

    def _merge_clock(self, db: RedisStateDB) -> None:
        if self.clock:
            clock_value = db.get_clock()
            new_value = self.clock.adjust(clock_value)
            db.set_clock(new_value)

    def sync(self) -> None:
        """Synchronize current state to Redis."""
        self._sync_with(self.db)

    def _sync_with(self, db: RedisStateDB) -> RedisStateDB:
        self._revoked_tasks.purge()
        self.db.update(
            zrevoked=self._revoked_tasks,
            clock=self.clock.forward() if self.clock else 0,
        )
        return db

    def save(self) -> None:
        """Save state and close connections."""
        try:
            logger.info("[redis-statedb] Saving worker state to Redis")
            self.sync()
            self.close()
        except Exception as exc:
            logger.error("[redis-statedb] Failed to save state: %s", exc)

    def close(self) -> None:
        """Close Redis connection."""
        try:
            self.db.close()
        except Exception as exc:
            logger.error("[redis-statedb] Failed to close Redis connection: %s", exc)

    @property
    def db(self) -> RedisStateDB:
        return self.redis_db

    @property
    def _revoked_tasks(self) -> LimitedSet:
        return self.state.revoked
