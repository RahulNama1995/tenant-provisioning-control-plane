.PHONY: up down test lint format check
up:
	docker compose up --build -d

down:
	docker compose down

test:
	pytest --cov=app --cov=worker --cov-report=term-missing

lint:
	ruff check .

format:
	ruff format .

check:
	ruff format .
	ruff check .
	bandit -q -r app worker
	pytest --cov=app --cov=worker --cov-report=term-missing
