import time

from celery import shared_task, states
from celery.result import AsyncResult
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(bind=True)
def add(self, x, y):
    """Add two numbers together."""
    logger.info(f"Adding {x} + {y}")
    result = x + y
    logger.info(f"Result: {result}")
    return result


@shared_task(bind=True)
def multiply(self, x, y):
    """Multiply two numbers."""
    logger.info(f"Multiplying {x} * {y}")
    result = x * y
    logger.info(f"Result: {result}")
    return result


@shared_task(bind=True)
def hello(self):
    """Simple hello world task."""
    logger.info("Hello from Celery!")
    return "Hello World!"


@shared_task(bind=True, track_started=True)
def long_running_task(self, duration=10):
    """Simulate a long-running task."""
    logger.info(f"Starting long task (duration: {duration}s)")
    task_id = self.request.id
    for i in range(duration):
        logger.info(f"Current state: {AsyncResult(task_id).status}")
        if AsyncResult(task_id).status == states.REVOKED:
            logger.warning(f"Task {task_id} was revoked!")
            return "Task was revoked"

        time.sleep(1)
        logger.info(f"Progress: {i + 1}/{duration}")

    logger.info("Long task completed")
    return f"Task completed after {duration} seconds"
