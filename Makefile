.PHONY: install lint test fmt run-bronze run-silver run-gold run-all clean

install:
	uv sync --extra dev

lint:
	uv run ruff check src tests

fmt:
	uv run ruff format src tests
	uv run ruff check --fix src tests

test:
	uv run pytest -v

# Defaults: tenant=all, dev env, full Q1-Q2 2025 range
TENANT ?= all
ENV ?= dev
START ?= 2025-01-01
END ?= 2025-06-30

run-bronze:
	uv run saas-pipeline run --layer bronze --tenant $(TENANT) --env $(ENV) --start-date $(START) --end-date $(END)

run-silver:
	uv run saas-pipeline run --layer silver --tenant $(TENANT) --env $(ENV) --start-date $(START) --end-date $(END)

run-gold:
	uv run saas-pipeline run --layer gold --tenant $(TENANT) --env $(ENV) --start-date $(START) --end-date $(END)

run-all:
	uv run saas-pipeline run --layer all --tenant $(TENANT) --env $(ENV) --start-date $(START) --end-date $(END)

clean:
	rm -rf data/bronze data/silver data/gold data/bronze_quarantine data/silver_quarantine data/shared
	rm -rf spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
