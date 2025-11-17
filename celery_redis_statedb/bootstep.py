import atexit
import logging
from typing import TYPE_CHECKING, Any

from celery import bootsteps
from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]

from celery_redis_statedb.state import RedisStateDB

if TYPE_CHECKING:
    # from celery.worker import WorkController
    from celery.apps.worker import Worker

logger = logging.getLogger(__name__)


class RedisPersistent:
    """Redis-based persistent state manager for Celery workers.

    This class provides the interface between Celery's state management
    and our Redis backend, ensuring revoked task state persists across
    worker restarts.

    Attributes:
        state: Worker state object
        redis_db: RedisStateDB instance
        clock: Logical clock for distributed coordination
    """

    def __init__(
        self,
        state: Any,
        redis_url: str,
        worker_name: str,
        clock: Any | None = None,
        key_prefix: str = "celery:worker:state:",
    ) -> None:
        """Initialize Redis persistent state.

        Args:
            state: Worker state object
            redis_url: Redis connection URL
            worker_name: Unique worker identifier (hostname or worker name)
            clock: Optional logical clock
            key_prefix: Base prefix for Redis keys
        """
        self.state = state
        self.clock = clock
        self.redis_db = RedisStateDB(
            redis_url=redis_url,
            worker_name=worker_name,
            key_prefix=key_prefix,
        )

        logger.info(
            "Initializing Redis persistent state for worker=%s from %s",
            worker_name,
            redis_url,
        )

        # Load existing state from Redis
        self.merge()

    def merge(self) -> None:
        """Merge existing Redis state into worker state.

        This is called on worker startup to restore revoked tasks
        from previous runs.
        """
        try:
            # Get revoked tasks from Redis
            revoked_tasks = self.redis_db.get_revoked()

            if revoked_tasks:
                logger.info("Loading %d revoked tasks from Redis", len(revoked_tasks))

                # Add to worker's revoked set
                for task_id in revoked_tasks:
                    self.state.revoked.add(task_id)

            # Merge clock if available
            if self.clock:
                clock_value = self.redis_db.get_clock()
                if clock_value > 0:
                    self.clock.adjust(clock_value)
                    logger.debug("Adjusted clock to %d", clock_value)

        except Exception as exc:
            logger.error("Failed to merge state from Redis: %s", exc)
            # Don't raise - allow worker to start even if Redis is unavailable

    def sync(self) -> None:
        """Synchronize current state to Redis.

        This writes the current revoked task set to Redis, ensuring
        persistence across restarts.
        """
        try:
            # Purge expired entries first
            self.state.revoked.purge()

            # Sync revoked tasks to Redis
            if isinstance(self.state.revoked, LimitedSet):
                self.redis_db.add_revoked_bulk(self.state.revoked)
            else:
                # Fallback for other set types
                for task_id in self.state.revoked:
                    self.redis_db.add_revoked(task_id)

            # Sync clock if available
            if self.clock:
                clock_value = self.clock.forward()
                self.redis_db.set_clock(clock_value)

            logger.debug("Synchronized state to Redis")

        except Exception as exc:
            logger.error("Failed to sync state to Redis: %s", exc)
            # Don't raise - allow worker to continue even if sync fails

    def save(self) -> None:
        """Save state and close connections.

        This is called on worker shutdown to ensure all state
        is persisted before the worker exits.
        """
        try:
            logger.info("Saving worker state to Redis")
            self.sync()
            self.close()
        except Exception as exc:
            logger.error("Failed to save state: %s", exc)

    def close(self) -> None:
        """Close Redis connection."""
        try:
            self.redis_db.close()
        except Exception as exc:
            logger.error("Failed to close Redis connection: %s", exc)


class RedisStatePersistence(bootsteps.Step):
    """Celery bootstep for Redis-based state persistence.

    This bootstep replaces Celery's default filesystem-based state
    persistence with Redis. Revoked tasks persist indefinitely in Redis
    (matching Celery's default behavior).

    Configuration:
        - CELERY_REDIS_STATE_KEY_PREFIX: Key prefix (default: 'celery:worker:state:')

    Usage:
        Add to Celery worker configuration:

        ```python
        from celery import Celery
        from celery_redis_statedb import RedisStatePersistence

        app = Celery('myapp')
        app.steps['worker'].add(RedisStatePersistence)
        ```
        ```bash
        celery -A myapp worker --statedb=redis://localhost:6379/0
        ```
    """

    def __init__(self, worker: Worker, **kwargs: Any) -> None:
        # Check if statedb is configured
        self.enabled = self._should_enable(worker)
        worker._persistence = None  # type: ignore[attr-defined]
        super().__init__(worker, **kwargs)

    def _should_enable(self, worker: Worker) -> bool:
        if not worker.statedb:  # type: ignore[attr-defined]
            return False

        statedb = worker.statedb  # type: ignore[attr-defined]
        if isinstance(statedb, str):
            return statedb.startswith(("redis://", "rediss://"))

        return False

    def create(self, worker: Worker) -> None:
        if not self.enabled:
            return

        # Get configuration
        redis_url = worker.statedb  # type: ignore[attr-defined]
        # Use hostname as unique worker identifier
        worker_name = worker.hostname  # type: ignore[attr-defined]
        key_prefix = getattr(
            worker.app.conf,  # type: ignore[attr-defined]
            "redis_state_key_prefix",
            "celery:worker:state:",
        )

        logger.info("Setting up Redis state persistence for worker=%s: %s", worker_name, redis_url)

        try:
            # Create persistence layer
            worker._persistence = RedisPersistent(  # type: ignore[attr-defined]
                state=worker.state,  # type: ignore[attr-defined]
                redis_url=redis_url,
                worker_name=worker_name,
                clock=worker.app.clock,  # type: ignore[attr-defined]
                key_prefix=key_prefix,
            )

            # Register save on exit
            atexit.register(worker._persistence.save)  # type: ignore[attr-defined]

            logger.info("Redis state persistence initialized successfully")

        except Exception as exc:
            logger.error("Failed to initialize Redis state persistence: %s", exc)
            # Set to None so worker can continue without persistence
            worker._persistence = None  # type: ignore[attr-defined]
            raise
