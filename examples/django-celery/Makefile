.PHONY: help build up up-debug down logs logs-workers migrate shell clean test-state-persistence workers-debug

help:
	@echo "Django Celery Redis StateDB Example"
	@echo ""
	@echo "Available commands:"
	@echo "  make build       - Build Docker images"
	@echo "  make up          - Start all services"
	@echo "  make up-debug    - Start all services with workers in debug mode"
	@echo "  make down        - Stop all services"
	@echo "  make logs        - View logs from all services"
	@echo "  make logs-workers - View logs from Celery workers only"
	@echo ""
	@echo "Advanced usage:"
	@echo "  CELERY_LOG_LEVEL=debug make up  - Start with custom log level (debug/info/warning/error)"
	@echo "  make migrate     - Run Django migrations"
	@echo "  make shell       - Open Django shell"
	@echo "  make createsuperuser - Create Django superuser"
	@echo "  make test-tasks  - Test task execution"
	@echo "  make test-state-persistence - Test Redis state persistence across worker restarts"
	@echo "  make inspect     - Inspect Celery workers"
	@echo "  make redis-cli   - Open Redis CLI"
	@echo "  make clean       - Remove all containers and volumes"

build:
	docker compose build

up:
	docker compose up -d
	@echo "Services started!"
	@echo "Django: http://localhost:8001"

up-debug:
	docker compose -f docker-compose.yml -f docker-compose.debug.yml up -d
	@echo "Services started in DEBUG mode!"
	@echo "Django: http://localhost:8001"
	@echo "Workers running with --loglevel=debug"

down:
	docker compose down

logs:
	docker compose logs -f

logs-workers:
	docker compose logs -f celery-worker-1 celery-worker-2

migrate:
	docker compose exec web .venv/bin/python manage.py migrate

shell:
	docker compose exec web .venv/bin/python manage.py shell

createsuperuser:
	docker compose exec web .venv/bin/python manage.py createsuperuser

test-tasks:
	@echo "Testing task execution..."
	@curl -s "http://localhost:8001/tasks/add/?x=10&y=20" | jq
	@sleep 2
	@curl -s "http://localhost:8001/tasks/multiply/?x=5&y=6" | jq
	@sleep 2
	@curl -s "http://localhost:8001/tasks/hello/" | jq

inspect:
	docker compose exec web .venv/bin/celery -A myproject inspect active
	@echo ""
	docker compose exec web .venv/bin/celery -A myproject inspect stats

redis-cli:
	docker compose exec redis redis-cli

test-state-persistence:
	@bash test_state_persistence.sh

clean:
	docker compose down -v
	@echo "All containers and volumes removed!"
