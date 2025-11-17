# Celery Redis StateDB

[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-89%25-green)](tests/)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Production-grade Redis-based state persistence for Celery workers in containerized environments.

## Problem

Celery workers store revoked task state between restarts in a local file using Python's `shelve` module. When deploying Celery workers to container orchestration platforms like AWS ECS, Kubernetes, or Docker Swarm, this state file is lost upon container restarts, causing revoked task information to be lost.

**Related Issue:** [celery/celery#4886](https://github.com/celery/celery/issues/4886)

## Solution

This package provides a Redis-based backend for Celery's worker state persistence, replacing the default filesystem-based shelve storage. It's designed to:

- ✅ Persist revoked task state across container restarts
- ✅ Isolate state per worker using unique key prefixes
- ✅ Eliminate concurrency issues through per-worker key isolation
- ✅ Handle Redis connection failures gracefully
- ✅ Provide production-grade error handling and retry logic
- ✅ Integrate seamlessly with existing Celery applications

## Features

- **Distributed State Persistence**: Store revoked tasks in Redis instead of local files
- **Per-Worker Isolation**: Each worker maintains its own state using unique key prefixes based on hostname
- **Concurrency-Safe**: No race conditions since workers never write to the same keys
- **Production-Ready**: Built-in retry logic, connection pooling, and comprehensive error handling
- **Easy Integration**: Simple Celery bootstep that works with existing applications
- **Configurable**: Support for custom key prefixes, TTL, and Redis configurations
- **Well-Tested**: 87% test coverage with unit and integration tests

## Installation

```bash
# Using pip
pip install celery-redis-statedb

# Using uv
uv add celery-redis-statedb
```

## Quick Start

### Method 1: Command Line (Easiest)

The easiest way to use Redis state persistence is via the `--statedb` command line option:

```bash
celery -A myapp worker --statedb=redis://localhost:6379/0
```

### Method 2: Programmatic Installation (Recommended)

Use the `install_redis_statedb()` convenience function in your app:

**Basic usage (with defaults):**
```python
from celery import Celery
from celery_redis_statedb import install_redis_statedb

app = Celery('myapp')

# Install Redis state persistence (replaces default file-based StateDB)
install_redis_statedb(app)

# Start worker with Redis statedb
# celery -A myapp worker --statedb=redis://localhost:6379/0
```

**With custom configuration:**
```python
from celery import Celery
from celery_redis_statedb import install_redis_statedb

app = Celery('myapp')

# Optional: Configure via app.conf
app.conf.redis_state_key_prefix = 'myapp:worker:state:'
app.conf.redis_state_ttl = 7200  # 2 hours

# Install bootstep
install_redis_statedb(app)

# Start worker
# celery -A myapp worker --statedb=redis://localhost:6379/0
```

### Method 3: Environment Variables

Configure additional settings via environment variables:

```bash
# Required: Redis URL for statedb
export REDIS_STATEDB_URL=redis://localhost:6379/0

# Optional: Custom key prefix (default: celery:worker:state:)
export CELERY_REDIS_STATE_KEY_PREFIX=myapp:worker:state:

# Optional: TTL for revoked tasks in seconds (default: 10800 = 3 hours)
export CELERY_REDIS_STATE_TTL=7200

# Start worker
celery -A myapp worker --statedb=$REDIS_STATEDB_URL --loglevel=info
```

## Configuration Options

Configuration can be set via Celery app configuration (`app.conf`) or environment variables:

| Configuration Key | Environment Variable | Default | Description |
|------------------|---------------------|---------|-------------|
| `redis_state_key_prefix` | `CELERY_REDIS_STATE_KEY_PREFIX` | `celery:worker:state:` | Base prefix for Redis keys (worker hostname is appended) |

**Example using app.conf:**
```python
app.conf.redis_state_key_prefix = 'myapp:worker:state:'
```

**Example using environment variables:**
```bash
export CELERY_REDIS_STATE_KEY_PREFIX=myapp:worker:state:
```

### Revoked Task Persistence

Revoked tasks persist **indefinitely** in Redis (matching Celery's default behavior with shelve). This ensures that once a task is revoked, it remains revoked even across worker restarts.

**Important:** Revoked tasks are NEVER automatically deleted by workers. This is intentional to prevent accidentally re-executing revoked tasks.

**Manual cleanup:** Use the `purge_old_revoked_tasks()` function to explicitly remove old revoked tasks when needed. This should be called periodically via a cron job or scheduled Celery task.

### Manual Cleanup of Old Revoked Tasks

The `purge_old_revoked_tasks()` function provides explicit control over cleaning up old revoked tasks. This function must be called manually - workers never automatically delete revoked tasks.

**Basic usage:**
```python
from datetime import timedelta
from celery import Celery
from celery_redis_statedb import purge_old_revoked_tasks

app = Celery('myapp')

# Remove revoked tasks older than 30 days from all workers
count = purge_old_revoked_tasks(app, older_than=timedelta(days=30))
print(f"Removed {count} old revoked tasks")
```

**Purge specific worker:**
```python
# Remove old tasks from a specific worker only
count = purge_old_revoked_tasks(
    app,
    older_than=timedelta(days=7),
    worker_name="worker1@hostname",
)
print(f"Removed {count} tasks from worker1@hostname")
```

**Using a scheduled Celery task:**
```python
from celery import Celery
from celery.schedules import crontab
from datetime import timedelta
from celery_redis_statedb import purge_old_revoked_tasks

app = Celery('myapp')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Purge old revoked tasks every Sunday at midnight
    sender.add_periodic_task(
        crontab(hour=0, minute=0, day_of_week=0),
        purge_old_revoked.s(),
    )

@app.task
def purge_old_revoked():
    """Scheduled task to clean up old revoked tasks."""
    count = purge_old_revoked_tasks(app, older_than=timedelta(days=30))
    return f"Purged {count} old revoked tasks"
```

**Using a cron job:**
```bash
# Add to crontab to run every Sunday at midnight
0 0 * * 0 /path/to/venv/bin/python -c "from myapp import app; from celery_redis_statedb import purge_old_revoked_tasks; from datetime import timedelta; purge_old_revoked_tasks(app, timedelta(days=30))"
```

**Parameters:**
- `app` (required): Celery application instance
- `older_than` (required): timedelta specifying minimum age of tasks to remove
- `worker_name` (optional): Specific worker hostname to purge (None = all workers)
- `redis_url` (optional): Redis URL (if None, uses app.conf.broker_url)

**Returns:** Total number of tasks removed across all workers

## Architecture

### Components

1. **RedisStateDB**: Core class that manages Redis operations for a specific worker
2. **RedisPersistent**: Persistence layer that integrates with Celery's state
3. **RedisStatePersistence**: Celery bootstep for automatic initialization

### Per-Worker Key Isolation

Each worker maintains its own state using a unique key prefix based on the worker's hostname:

```
celery:worker:state:<worker-hostname>:revoked
celery:worker:state:<worker-hostname>:clock
```

This design eliminates concurrency issues since:
- Workers never write to the same Redis keys
- No locks or atomic operations needed
- Simple, fast, and scalable
- Each worker independently manages its own state persistence

When a worker restarts, it loads its own state from its dedicated keys in Redis.

## Worker Hostname Configuration

### Default Hostname Behavior

By default, Celery generates worker hostnames in the format `celery@<system-hostname>`. In containerized environments:
- Docker: Uses the container ID (e.g., `celery@f9207bc94b22`)
- Kubernetes: Uses the pod name (e.g., `celery@worker-deployment-7d9f8b6c-xk2p5`)
- ECS: Uses the task ID or container hostname

**Each container automatically gets a unique hostname**, ensuring per-worker key isolation works correctly.

### Customizing Worker Hostnames

For better visibility and debugging, you can set custom worker hostnames using the `-n` or `--hostname` option:

```bash
# Static name with system hostname
celery -A myapp worker -n worker1@%h --statedb=redis://localhost:6379/0

# Using environment variable for replica number (Kubernetes)
celery -A myapp worker -n worker-$REPLICA_ID@%h --statedb=redis://localhost:6379/0

# Custom name with domain
celery -A myapp worker -n myworker@example.com --statedb=redis://localhost:6379/0
```

**Hostname Variables:**
- `%h` - Full hostname with domain (e.g., `george.example.com`)
- `%n` - Hostname only (e.g., `george`)
- `%d` - Domain only (e.g., `example.com`)

**Note:** In Supervisor config files, escape `%` as `%%` (e.g., `%%h`).

### Multiple Workers on Same Host

When running multiple workers on the same machine, ensure each has a unique hostname:

```bash
celery -A myapp worker -n worker1@%h --statedb=redis://localhost:6379/0
celery -A myapp worker -n worker2@%h --statedb=redis://localhost:6379/0
celery -A myapp worker -n worker3@%h --statedb=redis://localhost:6379/0
```

**⚠️ Important:** If multiple workers share the same hostname, they will overwrite each other's state in Redis. Celery will also show a `DuplicateNodenameWarning`. Always ensure unique worker hostnames in production.

## Container Deployments

### AWS ECS Deployment

Example ECS task definition with custom worker hostname:

```json
{
  "family": "celery-worker",
  "containerDefinitions": [
    {
      "name": "celery-worker",
      "image": "myapp:latest",
      "command": [
        "celery",
        "-A",
        "myapp",
        "worker",
        "-n", "worker@%h",
        "--statedb=redis://redis.cluster.local:6379/0",
        "--loglevel=info"
      ]
    }
  ]
}
```

**Best Practices for ECS:**
- Each task gets a unique hostname automatically (task ID)
- Set `stopTimeout` to allow graceful shutdown (e.g., 120 seconds for 2-minute tasks)
- Use ECS service auto-scaling based on queue depth

### Kubernetes Deployment

Example Kubernetes deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celery-worker
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: celery-worker
        image: myapp:latest
        command:
          - celery
          - -A
          - myapp
          - worker
          - -n
          - worker@$(HOSTNAME)
          - --statedb=redis://redis-service:6379/0
          - --loglevel=info
        env:
          - name: HOSTNAME
            valueFrom:
              fieldRef:
                fieldPath: metadata.name
        resources:
          requests:
            cpu: "1"
            memory: "1Gi"
```

**Best Practices for Kubernetes:**
- Each pod gets a unique name automatically
- Run single-core workers (`-c 1`) for better scaling and debugging
- Set `terminationGracePeriodSeconds` for graceful shutdown
- Use Horizontal Pod Autoscaler (HPA) for auto-scaling
- Implement liveness/readiness probes with `celery inspect ping`

### Docker Compose

Example docker-compose.yml:

```yaml
version: '3.8'
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"

  celery-worker:
    image: myapp:latest
    command: celery -A myapp worker -n worker@%h --statedb=redis://redis:6379/0
    depends_on:
      - redis
    deploy:
      replicas: 3
```

Each replica gets a unique container ID as its hostname automatically.

## Troubleshooting

### Verify Worker Hostnames

To check that each worker has a unique hostname, inspect active workers:

```bash
# List all active workers
celery -A myapp inspect active

# Check worker stats (shows hostname for each worker)
celery -A myapp inspect stats
```

### Check Redis Keys

To verify per-worker keys are being created correctly:

```bash
# Connect to Redis
redis-cli

# List all worker state keys
KEYS celery:worker:state:*:revoked

# Example output:
# 1) "celery:worker:state:worker1@host1:revoked"
# 2) "celery:worker:state:worker2@host2:revoked"
# 3) "celery:worker:state:worker3@host3:revoked"

# Check revoked tasks for a specific worker
ZRANGE celery:worker:state:worker1@host1:revoked 0 -1
```

### Common Issues

**Issue:** Workers overwriting each other's state
- **Cause:** Multiple workers using the same hostname
- **Solution:** Set unique hostnames with `-n` option (e.g., `-n worker1@%h`, `-n worker2@%h`)

**Issue:** `DuplicateNodenameWarning` on worker startup
- **Cause:** Another worker with the same nodename is already registered
- **Solution:** Ensure each worker has a unique hostname

**Issue:** State not persisting across restarts
- **Cause:** Worker hostname changes between restarts
- **Solution:** In containers, use stable hostnames (e.g., pod names in Kubernetes) or set custom static names

## Testing

```bash
# Quick check - run all quality checks at once
make check-all

# Individual commands
make test        # Run tests with coverage
make lint        # Run ruff linter
make typecheck   # Run mypy and ty type checkers
make format      # Format code with ruff
make clean       # Clean up temporary files

# Or run commands directly
uv run pytest --cov=celery_redis_statedb
uv run ruff check celery_redis_statedb/
uv run mypy celery_redis_statedb/
uv run ty check celery_redis_statedb/
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
