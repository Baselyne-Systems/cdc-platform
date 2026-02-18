.PHONY: up down build test-unit test-integration test-e2e lint fmt health clean

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

test-e2e:  ## E2E test: builds Docker images, boots infra, verifies health probes + connector
	./scripts/test-e2e.sh

bench:  ## Run benchmark tests (manages Docker lifecycle automatically)
	$(COMPOSE) up -d --wait
	BENCHMARK_SKIP_DOCKER=true uv run pytest tests/benchmark/test_backpressure.py -v -m benchmark -s --tb=short
	BENCHMARK_SKIP_DOCKER=true uv run pytest tests/benchmark/test_sink_latency.py -v -m benchmark -s --tb=short
	BENCHMARK_SKIP_DOCKER=true uv run pytest tests/benchmark/test_multi_partition.py -v -m benchmark -s --tb=short
	BENCHMARK_SKIP_DOCKER=true uv run pytest tests/benchmark/test_throughput.py -v -m benchmark -s --tb=short
	$(COMPOSE) down -v

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
