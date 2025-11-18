# Django + Celery + Redis StateDB Example

This example demonstrates how to use `celery-redis-statedb` with Django and Celery in a containerized environment using Docker Compose.

## Overview

This example includes:
- **Django 4.2+** - Web framework
- **Celery 5.3+** - Distributed task queue
- **Redis** - Message broker and state persistence
- **PostgreSQL** - Database backend
- **Flower** - Celery monitoring tool
- **celery-redis-statedb** - Redis-based state persistence for workers

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│   Django    │────▶│   Redis     │◀────│   Worker 1   │
│   Web App   │     │  (Broker)   │     │              │
└─────────────┘     └─────────────┘     └──────────────┘
                            │
                            │            ┌──────────────┐
                            └───────────▶│   Worker 2   │
                                         │              │
                    ┌─────────────┐      └──────────────┘
                    │   Redis     │
                    │  (StateDB)  │      ┌──────────────┐
                    └─────────────┘      │ Celery Beat  │
                                         │  (Scheduler) │
                    ┌─────────────┐      └──────────────┘
                    │ PostgreSQL  │
                    │  (Database) │      ┌──────────────┐
                    └─────────────┘      │   Flower     │
                                         │ (Monitoring) │
                                         └──────────────┘
```

## Features Demonstrated

1. **Worker State Persistence**: Revoked tasks persist in Redis across container restarts
2. **Multiple Workers**: Two Celery workers with unique hostnames
3. **Per-Worker Isolation**: Each worker maintains its own state using unique Redis keys
4. **Task Examples**: Various task types (simple, long-running, chains, periodic)
5. **HTTP Endpoints**: Trigger and monitor tasks via REST API
6. **Monitoring**: Flower dashboard for real-time monitoring

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Port 8000, 5555, and 6379 available

### 1. Start the Services

```bash
cd examples/django-celery

# Build and start all services
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build
```

This will start:
- **Django** (web): http://localhost:8000
- **Flower** (monitoring): http://localhost:5555
- **Redis**: localhost:6379
- **PostgreSQL**: localhost:5432
- **2 Celery Workers**: worker1 and worker2
- **Celery Beat**: Periodic task scheduler

### 2. Run Database Migrations

```bash
# In a new terminal
docker-compose exec web python manage.py migrate
```

### 3. Create a Superuser (Optional)

```bash
docker-compose exec web python manage.py createsuperuser
```

## Using the Example

### Trigger Tasks via HTTP

**Simple addition:**
```bash
curl "http://localhost:8000/tasks/add/?x=10&y=20"
# Returns: {"task_id": "...", "status": "Task triggered", "x": 10, "y": 20}
```

**Multiplication:**
```bash
curl "http://localhost:8000/tasks/multiply/?x=5&y=6"
```

**Hello world:**
```bash
curl "http://localhost:8000/tasks/hello/"
```

**Long-running task:**
```bash
curl "http://localhost:8000/tasks/long/?duration=30"
```

**Task chain:**
```bash
curl "http://localhost:8000/tasks/chain/?x=5"
# Computes: (5 * 2 + 10)² = 225
```

**Check task status:**
```bash
curl "http://localhost:8000/tasks/status/<task_id>/"
```

**Revoke a task:**
```bash
curl -X POST "http://localhost:8000/tasks/revoke/<task_id>/"
```

**List all available tasks:**
```bash
curl "http://localhost:8000/tasks/list/"
```

### Monitor with Flower

Open http://localhost:5555 in your browser to:
- View active/scheduled/succeeded/failed tasks
- Monitor worker status
- See task execution times and graphs
- Inspect task arguments and results

### Testing State Persistence

To verify that revoked tasks persist across restarts:

```bash
# 1. Start a long-running task
curl "http://localhost:8000/tasks/long/?duration=60"
# Save the task_id from the response

# 2. Revoke the task
curl -X POST "http://localhost:8000/tasks/revoke/<task_id>/"

# 3. Check Redis has the revoked task
docker-compose exec redis redis-cli
> KEYS myapp:worker:state:*
> GET myapp:worker:state:worker1@<hostname>:zrevoked

# 4. Restart the workers
docker-compose restart celery-worker-1 celery-worker-2

# 5. Try to run the same task again - it should remain revoked
# (Note: In practice, task IDs are unique, but the revoked state persists)

# 6. Verify in logs that workers loaded revoked tasks on startup
docker-compose logs celery-worker-1 | grep "revoked"
```

## Configuration

### Environment Variables

Key environment variables (set in `docker-compose.yml`):

```yaml
CELERY_BROKER_URL=redis://redis:6379/0          # Celery message broker
REDIS_STATEDB_URL=redis://redis:6379/1          # Worker state persistence
CELERY_REDIS_STATE_KEY_PREFIX=myapp:worker:state:  # Redis key prefix
DATABASE_URL=postgresql://...                    # PostgreSQL connection
```

### Worker Hostnames

Each worker has a unique hostname for state isolation:
```yaml
celery-worker-1:
  command: celery -A myproject worker -n worker1@%h --statedb=redis://redis:6379/1

celery-worker-2:
  command: celery -A myproject worker -n worker2@%h --statedb=redis://redis:6379/1
```

The `%h` placeholder is replaced with the container hostname, ensuring unique keys in Redis:
- `myapp:worker:state:worker1@container-id:zrevoked`
- `myapp:worker:state:worker2@container-id:zrevoked`

## Project Structure

```
django-celery/
├── docker-compose.yml      # Docker Compose configuration
├── Dockerfile              # Container image definition
├── requirements.txt        # Python dependencies
├── manage.py              # Django management script
├── myproject/             # Django project
│   ├── __init__.py        # Imports Celery app
│   ├── celery.py          # Celery configuration
│   ├── settings.py        # Django settings
│   ├── urls.py            # URL routing
│   └── wsgi.py            # WSGI entry point
└── tasks/                 # Tasks Django app
    ├── __init__.py
    ├── apps.py            # App configuration
    ├── tasks.py           # Celery task definitions
    ├── views.py           # HTTP endpoints
    └── urls.py            # URL routing for tasks
```

## Key Code Sections

### Celery Configuration (`myproject/celery.py`)

```python
from celery import Celery
from celery_redis_statedb import install_redis_statedb

app = Celery('myproject')
app.config_from_object('django.conf:settings', namespace='CELERY')

# Install Redis StateDB - this is the key integration!
install_redis_statedb(app)

app.autodiscover_tasks()
```

### Example Task (`tasks/tasks.py`)

```python
@shared_task(bind=True)
def long_running_task(self, duration=10):
    for i in range(duration):
        if self.is_revoked():  # Check if task was revoked
            return 'Task was revoked'
        time.sleep(1)
    return f'Completed after {duration} seconds'
```

## Scaling Workers

To add more workers:

```bash
docker-compose up --scale celery-worker-1=3
```

Or add more worker services in `docker-compose.yml`:

```yaml
celery-worker-3:
  build: .
  command: celery -A myproject worker -n worker3@%h --statedb=redis://redis:6379/1
  # ... rest of configuration
```

## Troubleshooting

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f celery-worker-1

# Just errors
docker-compose logs -f | grep ERROR
```

### Check Redis keys

```bash
docker-compose exec redis redis-cli

# List all worker state keys
KEYS myapp:worker:state:*

# View revoked tasks for a specific worker
GET myapp:worker:state:worker1@<container-id>:zrevoked

# View clock value
GET myapp:worker:state:worker1@<container-id>:clock
```

### Inspect worker status

```bash
# Active workers
docker-compose exec web celery -A myproject inspect active

# Registered tasks
docker-compose exec web celery -A myproject inspect registered

# Worker statistics
docker-compose exec web celery -A myproject inspect stats
```

### Reset everything

```bash
# Stop and remove all containers, volumes
docker-compose down -v

# Rebuild and start fresh
docker-compose up --build
```

## Production Considerations

This example is for **development and demonstration only**. For production:

1. **Security**:
   - Change `SECRET_KEY` in settings
   - Set `DEBUG=False`
   - Configure `ALLOWED_HOSTS`
   - Use strong database passwords
   - Enable Redis authentication
   - Use HTTPS/TLS

2. **Reliability**:
   - Use a production WSGI server (Gunicorn/uWSGI)
   - Configure worker concurrency (`-c` option)
   - Set up health checks and monitoring
   - Configure log aggregation
   - Use persistent volumes for PostgreSQL

3. **Performance**:
   - Use Redis Sentinel or Cluster for HA
   - Configure Redis persistence (AOF/RDB)
   - Tune worker pool settings
   - Set appropriate task time limits
   - Monitor memory usage

4. **Deployment**:
   - Use orchestration (Kubernetes, ECS, etc.)
   - Implement graceful shutdown
   - Set resource limits
   - Use secrets management
   - Configure auto-scaling

## Learn More

- [celery-redis-statedb Documentation](../../README.md)
- [Celery Documentation](https://docs.celeryq.dev/)
- [Django Documentation](https://docs.djangoproject.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## License

This example is provided as-is for educational purposes.
