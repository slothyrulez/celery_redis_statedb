# Django + Celery + Redis StateDB Example

Demonstrates Redis-based state persistence for Celery workers. Revoked tasks persist across container restarts.

> **Note**: Uses local development version of `celery-redis-statedb`.

## Quick Start

```bash
# 1. Start services
make build && make up

# 2. Run migrations
make migrate

# 3. Test it works
curl "http://localhost:8000/tasks/add/?x=10&y=20"
```

**What you get:**
- Django web app (http://localhost:8000)
- 2 Celery workers with Redis state persistence
- PostgreSQL database
- Celery Beat scheduler
- Local development with uv

## Key Commands

**Trigger tasks:**
```bash
curl "http://localhost:8000/tasks/hello/"
curl "http://localhost:8000/tasks/multiply/?x=5&y=6"
curl "http://localhost:8000/tasks/long/?duration=30"
curl "http://localhost:8000/tasks/chain/?x=5"
curl "http://localhost:8000/tasks/status/<task_id>/"
curl -X POST "http://localhost:8000/tasks/revoke/<task_id>/"
```

**Monitor:**
```bash
make logs                    # View all logs
make inspect                 # Inspect worker stats
docker-compose logs -f celery-worker-1
```

**Cleanup:**
```bash
make clean                   # Stop and remove everything
```

## Test State Persistence

```bash
# Start a long task
curl "http://localhost:8000/tasks/long/?duration=60"
# Returns: {"task_id": "abc123", ...}

# Revoke it
curl -X POST "http://localhost:8000/tasks/revoke/abc123/"

# Restart workers
docker-compose restart celery-worker-1 celery-worker-2

# Verify revoked state persisted
docker-compose logs celery-worker-1 | grep "revoked"
docker-compose exec redis redis-cli KEYS "myapp:worker:state:*"
```

## Configuration

Key integration in `myproject/celery.py`:
```python
from celery_redis_statedb import install_redis_statedb

app = Celery('myproject')
install_redis_statedb(app)  # Enables Redis state persistence
```

Environment variables in `docker-compose.yml`:
```yaml
CELERY_BROKER_URL=redis://redis:6379/0
REDIS_STATEDB_URL=redis://redis:6379/1
CELERY_REDIS_STATE_KEY_PREFIX=myapp:worker:state:
```

## Architecture

```
Django ──▶ Redis (Broker) ◀── Worker 1
              │                Worker 2
              ▼
        Redis (StateDB)
              ▼
        PostgreSQL
```

Each worker maintains its own state with unique Redis keys: `myapp:worker:state:worker1@host:zrevoked`

## Project Structure

```
django-celery/
├── docker-compose.yml      # Service orchestration
├── Dockerfile              # Container setup
├── pyproject.toml         # Dependencies (uv)
├── Makefile               # Helper commands
├── myproject/             # Django project
│   ├── celery.py          # Celery + Redis StateDB setup
│   └── settings.py        # Configuration
└── tasks/                 # Tasks app
    ├── tasks.py           # Task definitions
    └── views.py           # HTTP endpoints
```
