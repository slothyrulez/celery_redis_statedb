"""Example Celery application using Redis state persistence.

This example demonstrates how to configure a Celery application to use
Redis-based state persistence for containerized deployments.

Usage:
    # Start worker with Redis statedb
    celery -A example_app worker --statedb=redis://localhost:6379/0 --loglevel=info

    # With custom configuration via environment variables
    export CELERY_REDIS_STATE_KEY_PREFIX=example:worker:state:
    export CELERY_REDIS_STATE_TTL=7200
    celery -A example_app worker --statedb=redis://localhost:6379/0 --loglevel=info

    # Or run this script to test the configuration
    python example_app.py
"""

from celery import Celery

from celery_redis_statedb import install_redis_statedb

# Create Celery application
app = Celery(
    "example_app",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

# Configure Celery
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

# Optional: Configure Redis state persistence via app.conf
app.conf.redis_state_key_prefix = "example:worker:state:"
app.conf.redis_state_ttl = 7200  # 2 hours

# Install Redis state persistence (convenience function)
install_redis_statedb(app)


@app.task(bind=True)
def add(self, x: int, y: int) -> int:
    """Example task that adds two numbers."""
    print(f"Task {self.request.id}: Adding {x} + {y}")
    return x + y
