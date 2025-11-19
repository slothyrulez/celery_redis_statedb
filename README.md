# Celery Redis StateDB

[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-89%25-green)](tests/)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Redis-based state persistence for Celery workers in containerized environments.

This package provides a Redis-based backend for Celery's worker state persistence, replacing the default filesystem-based shelve storage. It's designed to:

- ✅ Persist revoked task state across container restarts
- ✅ Isolate state per worker using unique key prefixes
- ✅ Eliminate concurrency issues through per-worker key isolation
- ✅ Handle Redis connection failures gracefully
- ✅ Integrate seamlessly with existing Celery applications

## Features

- **Distributed State Persistence**: Store revoked tasks in Redis instead of local files
- **Per-Worker Isolation**: Each worker maintains its own state using unique key prefixes based on hostname
- **Concurrency-Safe**: No race conditions since workers never write to the same keys
- **Easy Integration**: Simple Celery bootstep that works with existing applications

## Installation

```bash
# Using pip
pip install celery-redis-statedb

# Using uv
uv add celery-redis-statedb
```

## Quick Start

### Method 1: Command Line (Easiest)

The easiest way to use Redis state persistence is via the `--redis-statedb` command line option:

```bash
celery -A myapp worker --redis-statedb=redis://localhost:6379/0
```

### Method 2: Programmatic Installation (Recommended)

Use the `install_redis_statedb()` convenience function in your app:

**Basic usage (with defaults):**
```python
from celery import Celery
from celery_redis_statedb import install_redis_statedb

app = Celery('myapp')

# Install Redis state persistence
install_redis_statedb(app)

# Start worker with Redis statedb
# celery -A myapp worker --redis-statedb=redis://localhost:6379/0
```

**With custom configuration:**
```python
from celery import Celery
from celery_redis_statedb import install_redis_statedb

app = Celery('myapp')

# Optional: Configure via app.conf
app.conf.redis_state_key_prefix = 'myapp:worker:state:'

# Install bootstep
install_redis_statedb(app)

# Start worker
# celery -A myapp worker --redis-statedb=redis://localhost:6379/0
```

### Method 3: Environment Variables

Configure additional settings via environment variables:

```bash
# Optional: Custom key prefix (default: celery:worker:state:)
export CELERY_REDIS_STATE_KEY_PREFIX=myapp:worker:state:

# Start worker
celery -A myapp worker --redis-statedb=redis://localhost:6379/0 --loglevel=info
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

Celery's `LimitedSet` automatically purges expired items based on the `maxlen` parameter (default: 10000 items). When the set reaches this limit, the oldest items are automatically removed to maintain the size constraint.

## Architecture

### Per-Worker Key Isolation

Each worker maintains its own state using a unique key prefix based on the worker's hostname:

```
celery:worker:state:<worker-hostname>:zrevoked
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
celery -A myapp worker -n worker1@%h --redis-statedb=redis://localhost:6379/0

# Using environment variable for replica number (Kubernetes)
celery -A myapp worker -n worker-$REPLICA_ID@%h --redis-statedb=redis://localhost:6379/0

# Custom name with domain
celery -A myapp worker -n myworker@example.com --redis-statedb=redis://localhost:6379/0
```

**Hostname Variables:**
- `%h` - Full hostname with domain (e.g., `george.example.com`)
- `%n` - Hostname only (e.g., `george`)
- `%d` - Domain only (e.g., `example.com`)

**Note:** In Supervisor config files, escape `%` as `%%` (e.g., `%%h`).

### Multiple Workers on Same Host

When running multiple workers on the same machine, ensure each has a unique hostname:

```bash
celery -A myapp worker -n worker1@%h --redis-statedb=redis://localhost:6379/0
celery -A myapp worker -n worker2@%h --redis-statedb=redis://localhost:6379/0
celery -A myapp worker -n worker3@%h --redis-statedb=redis://localhost:6379/0
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
        "--redis-statedb=redis://redis.cluster.local:6379/0",
        "--loglevel=info"
      ]
    }
  ]
}
```

**Best Practices for ECS:**
- Set `stopTimeout` to allow graceful shutdown (e.g., 120 seconds for 2-minute tasks)


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
KEYS celery:worker:state:*:zrevoked

# Example output:
# 1) "celery:worker:state:worker1@host1:zrevoked"
# 2) "celery:worker:state:worker2@host2:zrevoked"
# 3) "celery:worker:state:worker3@host3:zrevoked"

# Check revoked tasks for a specific worker
GET celery:worker:state:worker1@host1:zrevoked
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
- **Solution:** In containers, use stable hostnames or set custom static names

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

## Documentation

For detailed information about Celery's state persistence format and how this package implements it:

- [Celery Shelve Format Documentation](docs/CELERY_SHELVE_FORMAT.md) - Comprehensive guide to what Celery stores in its state database
