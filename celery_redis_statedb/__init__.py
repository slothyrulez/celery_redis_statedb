import logging
from typing import TYPE_CHECKING

from click import Option

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

This package provides Redis backend for Celery's worker state
persistence.
"""

logger = logging.getLogger(__name__)


def install_redis_statedb(app: "Celery") -> None:
    """Install Redis StateDB on a Celery app.

    This function adds the --redis-statedb CLI option and registers the
    Redis StateDB bootstep.

    Args:
        app: The Celery application instance.

    Example:
        >>> from celery import Celery
        >>> from celery_redis_statedb import install_redis_statedb
        >>>
        >>> app = Celery('myapp')
        >>> install_redis_statedb(app)
        >>>
        >>> # Start worker with Redis statedb:
        >>> # celery -A myapp worker --redis-statedb=redis://localhost:6379/0

    """
    # Add the --redis-statedb option to worker command using Celery's extension API
    assert app.user_options is not None, "Celery app.user_options is not initialized"
    app.user_options["worker"].add(
        Option(
            ("--redis-statedb",),
            type=str,
            default=None,
            help="Redis URL for state persistence (e.g., redis://localhost:6379/0). ",
        )
    )

    logger.debug("[redis-statedb] Registered --redis-statedb CLI option for worker command")

    # Add Redis-based StateDB
    assert app.steps is not None, "Celery app.steps is not initialized"
    app.steps["worker"].add(RedisStatePersistence)

    logger.info("[redis-statedb] Installed successfully on %s", app.main or "app")  # type: ignore[attr-defined]
