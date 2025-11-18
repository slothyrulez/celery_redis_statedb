"""Celery tasks for demonstration."""

import time
import random
from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(bind=True)
def add(self, x, y):
    """Add two numbers together."""
    logger.info(f'Adding {x} + {y}')
    result = x + y
    logger.info(f'Result: {result}')
    return result


@shared_task(bind=True)
def multiply(self, x, y):
    """Multiply two numbers."""
    logger.info(f'Multiplying {x} * {y}')
    result = x * y
    logger.info(f'Result: {result}')
    return result


@shared_task(bind=True)
def hello(self):
    """Simple hello world task."""
    logger.info('Hello from Celery!')
    return 'Hello World!'


@shared_task(bind=True)
def long_running_task(self, duration=10):
    """Simulate a long-running task."""
    logger.info(f'Starting long task (duration: {duration}s)')

    for i in range(duration):
        if self.is_revoked():
            logger.warning(f'Task {self.request.id} was revoked!')
            return 'Task was revoked'

        time.sleep(1)
        logger.info(f'Progress: {i+1}/{duration}')

    logger.info('Long task completed')
    return f'Task completed after {duration} seconds'


@shared_task(bind=True, max_retries=3)
def unreliable_task(self):
    """Task that randomly fails to demonstrate retry behavior."""
    logger.info('Running unreliable task')

    if random.random() < 0.5:
        logger.error('Task failed randomly!')
        raise Exception('Random failure')

    logger.info('Task succeeded')
    return 'Success!'


@shared_task(bind=True)
def chain_example_1(self, x):
    """First task in a chain example."""
    logger.info(f'Chain task 1: input={x}')
    result = x * 2
    logger.info(f'Chain task 1: result={result}')
    return result


@shared_task(bind=True)
def chain_example_2(self, x):
    """Second task in a chain example."""
    logger.info(f'Chain task 2: input={x}')
    result = x + 10
    logger.info(f'Chain task 2: result={result}')
    return result


@shared_task(bind=True)
def chain_example_3(self, x):
    """Third task in a chain example."""
    logger.info(f'Chain task 3: input={x}')
    result = x ** 2
    logger.info(f'Chain task 3: result={result}')
    return result
