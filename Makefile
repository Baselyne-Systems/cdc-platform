.PHONY: up down build test-unit test-integration lint fmt health clean

COMPOSE := docker compose -f docker/docker-compose.yml

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

build:
	$(COMPOSE) build connect

test-unit:
	uv run pytest tests/unit/ -v

test-integration:  ## Manages Docker lifecycle automatically — do not run `make up` first
	uv run pytest tests/integration/ -v -m integration

lint:
	uv run ruff check src/ tests/
	uv run mypy src/

fmt:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

health:
	uv run cdc health

clean: down
	$(COMPOSE) down -v --remove-orphans
