.PHONY: help test lint typecheck format check-all clean

help:
	@echo "Available commands:"
	@echo "  make test        - Run tests with coverage"
	@echo "  make lint        - Run ruff linter"
	@echo "  make typecheck   - Run mypy and ty type checkers"
	@echo "  make format      - Format code with ruff"
	@echo "  make check-all   - Run all checks (lint, typecheck, test)"
	@echo "  make clean       - Clean up temporary files"

test:
	uv run pytest --cov=celery_redis_statedb --cov-report=term-missing

lint:
	uv run ruff check celery_redis_statedb/ tests/

typecheck:
	@echo "Running mypy..."
	uv run mypy celery_redis_statedb/
	@echo "\nRunning ty..."
	uv run ty check celery_redis_statedb/
	uv run ty check tests/

format:
	uv run ruff format celery_redis_statedb/ tests/
	uv run ruff check --fix celery_redis_statedb/ tests/

check-all: lint typecheck test
	@echo "\n✅ All checks passed!"

clean:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
