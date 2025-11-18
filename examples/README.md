# celery-redis-statedb Examples

This directory contains real-world examples of using `celery-redis-statedb` in different scenarios.

## Available Examples

### Django + Celery + Docker Compose

**Location**: [`django-celery/`](./django-celery/)

A complete, production-ready example demonstrating:
- Django 4.2+ web application
- Multiple Celery workers with unique hostnames
- Redis for both message broker and state persistence
- PostgreSQL database
- Flower monitoring dashboard
- Docker Compose orchestration
- HTTP endpoints for triggering and monitoring tasks
- Automated test script for verifying state persistence

**Best for**: Django users, containerized deployments, learning how to integrate the package

**Quick start**:
```bash
cd django-celery
docker-compose up --build
```

See the [full README](./django-celery/README.md) for detailed instructions.

## Coming Soon

Additional examples that could be added:
- Flask + Celery + Kubernetes
- FastAPI + Celery + AWS ECS
- Plain Celery (without web framework)
- Celery + RabbitMQ broker + Redis statedb
- Multi-tenant setup with namespace isolation

## Contributing Examples

Have a great example to share? Contributions are welcome! Please ensure your example:

1. Is complete and self-contained
2. Includes a comprehensive README
3. Uses Docker/Docker Compose for easy setup
4. Demonstrates best practices
5. Includes comments explaining key concepts
6. Works out of the box

## Support

If you have questions about these examples:
- Check the main [project README](../README.md)
- Review the example-specific README
- Open an issue on GitHub
