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

    This bootstep adds Celery state persistence with Redis.
    Revoked tasks persist indefinitely in Redis.

    Usage:
        Add to Celery worker configuration:

        ```python
        from celery import Celery
        from celery_redis_statedb import install_redis_statedb

        app = Celery('myapp')
        install_redis_statedb(app)
        ```
        ```bash
        celery -A myapp worker --redis-statedb=redis://localhost:6379/0
        ```
    """

    def __init__(self, worker: "Worker", redis_statedb: str | None = None, **kwargs: Any) -> None:
        # Store redis_statedb for later use
        self.redis_statedb = redis_statedb
        # Check if statedb is configured
        self.enabled = self._should_enable(worker, redis_statedb)
        # worker._persistence = None  # type: ignore[attr-defined]
        super().__init__(worker, **kwargs)

    def _should_enable(self, worker: "Worker", redis_statedb: str | None) -> bool:
        # Prioritize --redis-statedb over --statedb
        if redis_statedb:
            return redis_statedb.startswith(("redis://", "rediss://"))

        return False

    def create(self, worker: "Worker") -> None:
        if not self.enabled:
            return

        # Use redis_statedb if provided
        redis_url = self.redis_statedb
        assert redis_url is not None, "redis_url must be set when enabled"
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

        logger.info(
            "[redis-statedb] Setting up persistence for worker=%s: %s",
            worker_name,
            redis_url,
        )

        try:
            # Create persistence layer with Celery-compatible API
            worker._redis_persistence = RedisPersistent(  # type: ignore[attr-defined]
                worker_name=worker_name,
                key_prefix=key_prefix,
                state=worker.state,  # type: ignore[attr-defined]
                redis_url=redis_url,
                clock=worker.app.clock,  # type: ignore[attr-defined]
            )
            atexit.register(worker._redis_persistence.save)  # type: ignore[attr-defined]
            logger.info("[redis-statedb] State persistence initialized successfully")

        except Exception as exc:
            logger.error("[redis-statedb] Failed to initialize state persistence: %s", exc)
            # Set to None so worker can continue without persistence
            worker._redis_persistence = None  # type: ignore[attr-defined]
            raise
