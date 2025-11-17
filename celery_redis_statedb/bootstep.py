import atexit
import logging
import os
from typing import TYPE_CHECKING, Any

from celery import bootsteps

from celery_redis_statedb.state import RedisPersistent

if TYPE_CHECKING:
    from celery.apps.worker import Worker

logger = logging.getLogger(__name__)


class RedisStatePersistence(bootsteps.Step):
    """Celery bootstep for Redis-based state persistence.

    This bootstep replaces Celery's default filesystem-based state
    persistence with Redis. Revoked tasks persist indefinitely in Redis.

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

        redis_url = worker.statedb  # type: ignore[attr-defined]
        worker_name = worker.hostname  # type: ignore[attr-defined]

        # Check environment variable first, then app.conf, then default
        key_prefix = os.environ.get(
            "CELERY_REDIS_STATE_KEY_PREFIX",
            getattr(
                worker.app.conf,  # type: ignore[attr-defined]
                "redis_state_key_prefix",
                "celery:worker:state:",
            ),
        )

        logger.info("Setting up Redis state persistence for worker=%s: %s", worker_name, redis_url)

        try:
            # Set worker name and key prefix on state for RedisPersistent to use
            worker.state._worker_name = worker_name  # type: ignore[attr-defined]
            worker.state._redis_key_prefix = key_prefix  # type: ignore[attr-defined]

            # Create persistence layer with Celery-compatible API
            worker._persistence = RedisPersistent(  # type: ignore[attr-defined]
                worker_name=worker_name,
                key_prefix=key_prefix,
                state=worker.state,  # type: ignore[attr-defined]
                redis_url=redis_url,
                clock=worker.app.clock,  # type: ignore[attr-defined]
            )
            atexit.register(worker._persistence.save)  # type: ignore[attr-defined]
            logger.info("Redis state persistence initialized successfully")

        except Exception as exc:
            logger.error("Failed to initialize Redis state persistence: %s", exc)
            # Set to None so worker can continue without persistence
            worker._persistence = None  # type: ignore[attr-defined]
            raise
