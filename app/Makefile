.PHONY: run prod lint format test typecheck check

run:
	uv run python run.py

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest

typecheck:
	uv run mypy .

check: format lint typecheck test
