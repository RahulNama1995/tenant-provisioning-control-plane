.PHONY: up down test lint format check
up:
	docker compose up --build -d

down:
	docker compose down

test:
	DATABASE_URL=postgresql+psycopg://control:control@postgres-test:5432/control_plane_test python -m pytest --cov=app --cov=worker --cov-report=term-missing

lint:
	ruff check .

format:
	ruff format .

check:
	ruff format --check .
	ruff check .
	bandit -ll -r app worker
	DATABASE_URL=postgresql+psycopg://control:control@postgres-test:5432/control_plane_test python -m pytest --cov=app --cov=worker --cov-report=term-missing
