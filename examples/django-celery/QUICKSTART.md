# Quick Start (30 seconds!)

Get the Django + Celery + Redis StateDB example running in 3 commands:

```bash
# 1. Start all services
docker-compose up -d --build

# 2. Run database migrations
docker-compose exec web python manage.py migrate

# 3. Test it works!
curl "http://localhost:8000/tasks/add/?x=10&y=20"
```

## What You Get

- **Django web app**: http://localhost:8000
- **Flower monitoring**: http://localhost:5555
- **2 Celery workers** with Redis state persistence
- **Periodic tasks** running automatically

## Quick Test Commands

```bash
# Trigger different tasks
curl "http://localhost:8000/tasks/hello/"
curl "http://localhost:8000/tasks/multiply/?x=5&y=6"
curl "http://localhost:8000/tasks/chain/?x=5"

# Check task status (replace <task_id>)
curl "http://localhost:8000/tasks/status/<task_id>/"

# View logs
docker-compose logs -f celery-worker-1

# Open Flower dashboard
open http://localhost:5555
```

## Test State Persistence

Run the automated test:

```bash
./test_state_persistence.sh
```

This will:
1. Start a long task
2. Revoke it
3. Restart workers
4. Verify revoked state persisted

## Cleanup

```bash
docker-compose down -v
```

---

👉 **Full documentation**: See [README.md](./README.md) for complete details.
