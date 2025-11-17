import logging
import time
from typing import Any

import redis
from celery.exceptions import ImproperlyConfigured
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]

"""Redis-based state database implementation."""

logger = logging.getLogger(__name__)

E_REDIS_MISSING = """
You need to install the redis library in order to use \
the Redis as statedb.
"""


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
        if redis is None:
            raise ImproperlyConfigured(E_REDIS_MISSING.strip())

        self.redis_url = redis_url
        self.worker_name = worker_name
        # Include worker name in key prefix for isolation
        self.key_prefix = f"{key_prefix}{worker_name}:"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Initialize Redis connection
        self.redis_client = redis.from_url(
            redis_url,
            decode_responses=False,  # We handle serialization ourselves
            socket_connect_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
        )

        logger.info(
            "Initialized RedisStateDB for worker=%s, prefix=%s",
            self.worker_name,
            self.key_prefix,
        )

    def _get_key(self, name: str) -> str:
        """Get full Redis key with prefix."""
        return f"{self.key_prefix}{name}"

    def _retry_operation(self, operation: Any, *args: Any, **kwargs: Any) -> Any:
        """Retry a Redis operation with exponential backoff.

        Args:
            operation: The Redis operation to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            redis.RedisError: If all retries fail
        """
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as exc:
                last_exception = exc
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        "Redis operation failed (attempt %d/%d), retrying in %.2fs: %s",
                        attempt + 1,
                        self.max_retries,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Redis operation failed after %d attempts: %s",
                        self.max_retries,
                        exc,
                    )
        raise last_exception  # type: ignore

    def add_revoked(self, task_id: str, timestamp: float | None = None) -> None:
        """Add a revoked task ID to Redis.

        Revoked tasks persist indefinitely (matching Celery's default behavior).

        Args:
            task_id: The task ID to add
            timestamp: Optional timestamp (defaults to current time)
        """
        if timestamp is None:
            timestamp = time.time()

        key = self._get_key("revoked")

        try:
            # Add task to sorted set (no concurrency issues - per-worker keys)
            self._retry_operation(self.redis_client.zadd, key, {task_id: timestamp})

            logger.debug("Added revoked task: %s", task_id)
        except redis.RedisError as exc:
            logger.error("Failed to add revoked task %s: %s", task_id, exc)
            raise

    def add_revoked_bulk(self, revoked_set: LimitedSet) -> None:
        """Add multiple revoked tasks to Redis efficiently.

        Revoked tasks persist indefinitely (matching Celery's default behavior).

        Args:
            revoked_set: LimitedSet of revoked tasks
        """
        if not revoked_set:
            return

        key = self._get_key("revoked")
        timestamp = time.time()

        try:
            # Prepare data for bulk insert
            mapping = {}
            for item in revoked_set:
                # LimitedSet stores (value, timestamp) tuples
                if isinstance(item, tuple):
                    task_id, task_time = item
                else:
                    task_id, task_time = item, timestamp
                mapping[task_id] = task_time

            if not mapping:
                return

            # Add all tasks to sorted set (no concurrency issues - per-worker keys)
            self._retry_operation(self.redis_client.zadd, key, mapping)

            logger.debug("Added %d revoked tasks in bulk", len(mapping))
        except redis.RedisError as exc:
            logger.error("Failed to add revoked tasks in bulk: %s", exc)
            raise

    def is_revoked(self, task_id: str) -> bool:
        """Check if a task ID is revoked.

        Args:
            task_id: The task ID to check

        Returns:
            True if the task is revoked, False otherwise
        """
        key = self._get_key("revoked")

        try:
            score = self._retry_operation(self.redis_client.zscore, key, task_id)
            return score is not None
        except redis.RedisError as exc:
            logger.error("Failed to check if task %s is revoked: %s", task_id, exc)
            # On error, assume not revoked to avoid blocking tasks
            return False

    def get_revoked(self) -> set[str]:
        """Get all revoked task IDs.

        Returns:
            Set of revoked task IDs
        """
        key = self._get_key("revoked")

        try:
            # Get all members from sorted set
            members = self._retry_operation(self.redis_client.zrange, key, 0, -1)
            # Decode bytes to strings
            return {m.decode("utf-8") if isinstance(m, bytes) else m for m in members}
        except redis.RedisError as exc:
            logger.error("Failed to get revoked tasks: %s", exc)
            return set()

    def purge_old_revoked(self, older_than_seconds: int) -> int:
        """Remove revoked tasks older than the specified age.

        This is a manual cleanup operation. By default, revoked tasks persist
        indefinitely (matching Celery's behavior).

        Args:
            older_than_seconds: Remove tasks older than this many seconds

        Returns:
            Number of tasks removed
        """
        key = self._get_key("revoked")
        timestamp = time.time()
        cutoff = timestamp - older_than_seconds

        try:
            count: int = self._retry_operation(
                self.redis_client.zremrangebyscore, key, "-inf", cutoff
            )
            if count > 0:
                logger.info(
                    "Purged %d revoked tasks older than %d seconds", count, older_than_seconds
                )
            return count
        except redis.RedisError as exc:
            logger.error("Failed to purge old revoked tasks: %s", exc)
            return 0

    def clear_revoked(self) -> None:
        """Clear all revoked tasks."""
        key = self._get_key("revoked")

        try:
            self._retry_operation(self.redis_client.delete, key)
            logger.info("Cleared all revoked tasks")
        except redis.RedisError as exc:
            logger.error("Failed to clear revoked tasks: %s", exc)
            raise

    def get_clock(self) -> int:
        """Get the logical clock value.

        Returns:
            Current clock value
        """
        key = self._get_key("clock")

        try:
            value = self._retry_operation(self.redis_client.get, key)
            return int(value) if value else 0
        except (redis.RedisError, ValueError) as exc:
            logger.error("Failed to get clock value: %s", exc)
            return 0

    def set_clock(self, value: int) -> None:
        """Set the logical clock value.

        Args:
            value: Clock value to set
        """
        key = self._get_key("clock")

        try:
            self._retry_operation(self.redis_client.set, key, value)
            logger.debug("Set clock value to %d", value)
        except redis.RedisError as exc:
            logger.error("Failed to set clock value: %s", exc)
            raise

    def increment_clock(self) -> int:
        """Increment and return the logical clock value.

        Returns:
            New clock value
        """
        key = self._get_key("clock")

        try:
            new_value = self._retry_operation(self.redis_client.incr, key)
            return int(new_value)
        except redis.RedisError as exc:
            logger.error("Failed to increment clock: %s", exc)
            raise

    def close(self) -> None:
        """Close Redis connection."""
        try:
            self.redis_client.close()
            logger.info("Closed Redis connection")
        except Exception as exc:
            logger.error("Error closing Redis connection: %s", exc)

    def ping(self) -> bool:
        """Check if Redis connection is alive.

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            result: bool = self._retry_operation(self.redis_client.ping)
            return result
        except redis.RedisError:
            return False
