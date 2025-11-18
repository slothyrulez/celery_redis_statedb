.PHONY: help build up down logs migrate shell test clean

help:
	@echo "Django Celery Redis StateDB Example"
	@echo ""
	@echo "Available commands:"
	@echo "  make build       - Build Docker images"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make logs        - View logs from all services"
	@echo "  make migrate     - Run Django migrations"
	@echo "  make shell       - Open Django shell"
	@echo "  make createsuperuser - Create Django superuser"
	@echo "  make test-tasks  - Test task execution"
	@echo "  make inspect     - Inspect Celery workers"
	@echo "  make redis-cli   - Open Redis CLI"
	@echo "  make clean       - Remove all containers and volumes"

build:
	docker compose build

up:
	docker compose up -d
	@echo "Services started!"
	@echo "Django: http://localhost:8001"

down:
	docker compose down

logs:
	docker compose logs -f

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

clean:
	docker compose down -v
	@echo "All containers and volumes removed!"
