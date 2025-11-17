import logging
from typing import TYPE_CHECKING

from celery_redis_statedb.bootstep import RedisStatePersistence
from celery_redis_statedb.state import RedisPersistent, RedisStateDB

if TYPE_CHECKING:
    from celery import Celery

__version__ = "0.1.0"
__all__ = [
    "RedisStateDB",
    "RedisStatePersistence",
    "RedisPersistent",
    "install_redis_statedb",
]

"""Redis-based state persistence for Celery workers.

This package provides a production-grade Redis backend for Celery's worker state
persistence, replacing the default filesystem-based shelve storage. It's designed
for containerized environments like AWS ECS where local file persistence is lost
on container restarts.
"""

logger = logging.getLogger(__name__)


def install_redis_statedb(app: Celery) -> None:
    """Install Redis StateDB on a Celery app, replacing the default file-based StateDB.

    This convenience function automatically removes the default filesystem-based
    StateDB bootstep and installs the Redis-based StateDB bootstep.

    Args:
        app: Celery application instance

    Example:
        ```python
        from celery import Celery
        from celery_redis_statedb import install_redis_statedb

        app = Celery('myapp')
        install_redis_statedb(app)

        # Start worker with Redis statedb
        # celery -A myapp worker --statedb=redis://localhost:6379/0
        ```

    Note:
        You still need to provide the --statedb=redis://... argument when starting
        the worker, or configure it via environment variables. This function only
        registers the bootstep.
    """
    try:
        from celery.worker.components import StateDB as DefaultStateDB

        # Remove default file-based StateDB
        if DefaultStateDB in app.steps["worker"]:
            app.steps["worker"].discard(DefaultStateDB)
            logger.debug("Removed default file-based StateDB")

        # Add Redis-based StateDB
        app.steps["worker"].add(RedisStatePersistence)

        logger.info("Redis StateDB installed successfully on %s", app.main or "app")  # type: ignore[attr-defined]
    except ImportError as exc:
        logger.error("Failed to import Celery components: %s", exc)
        raise
    except Exception as exc:
        logger.error("Failed to install Redis StateDB: %s", exc)
        raise
