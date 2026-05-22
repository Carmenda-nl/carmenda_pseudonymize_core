.PHONY: dev lint format test typecheck

dev:
	uv run fastapi dev

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest

typecheck:
	uv run mypy .
