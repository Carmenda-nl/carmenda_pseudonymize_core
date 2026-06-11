.PHONY: run prod lint format test typecheck

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
