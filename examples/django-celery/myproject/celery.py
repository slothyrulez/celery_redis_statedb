"""Celery configuration for Django project."""

import os

from celery import Celery
from celery_redis_statedb import install_redis_statedb

# Set the default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings')

# Create Celery app
app = Celery('myproject')

# Load config from Django settings (using CELERY_ prefix)
app.config_from_object('django.conf:settings', namespace='CELERY')

# Install Redis StateDB to replace default file-based persistence
# This ensures revoked tasks persist across container restarts
install_redis_statedb(app)

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task that prints its own request."""
    print(f'Request: {self.request!r}')
