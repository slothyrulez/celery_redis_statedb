FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set up directory structure to match the repository layout
# This allows the relative path in pyproject.toml to work as-is
WORKDIR /workspace

# Copy parent project (celery-redis-statedb)
COPY celery_redis_statedb ./celery_redis_statedb
COPY pyproject.toml README.md ./

# Copy example app to examples/django-celery
COPY examples/django-celery ./examples/django-celery

# Set working directory to the example app
WORKDIR /workspace/examples/django-celery

# Install dependencies using uv
# Note: We only install dependencies, not the project itself since it's a Django app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project

# Create a non-root user
# Note: Using UID 1000 which typically matches the host user for development
RUN useradd -m -u 1000 celeryuser && \
    chown -R celeryuser:celeryuser /workspace

USER celeryuser

# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Run migrations on startup (for web service)
# CMD will be overridden by docker-compose
CMD ["uv", "run", "python", "manage.py", "runserver", "0.0.0.0:8000"]
