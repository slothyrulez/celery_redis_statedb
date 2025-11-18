import os

from celery import Celery
from celery_redis_statedb import install_redis_statedb, install_redis_statedb_option

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

app = Celery("myproject")

app.config_from_object("django.conf:settings", namespace="CELERY")

# Install Redis StateDB to replace default file-based persistence
install_redis_statedb(app)

# Add --redis-statedb CLI option
install_redis_statedb_option(app)

app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task that prints its own request."""
    print(f"Request: {self.request!r}")
