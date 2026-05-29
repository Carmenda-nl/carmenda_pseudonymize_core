.PHONY: dev prod lint format test typecheck

dev:
	uv run fastapi dev

prod:
	uv run fastapi run

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest

typecheck:
	uv run mypy .
