# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

APP_DIR := app

.PHONY: help run lint format test typecheck check

help:
	@echo --------------------------------
	@echo CARMENDA DEDUCE ENGINE COMMANDS:
	@echo --------------------------------
	@echo   make run              Start the dev server
	@echo   make lint             Run ruff lint checks
	@echo   make format           Format code with ruff
	@echo   make test             Run the test suite
	@echo   make typecheck        Run mypy type checks
	@echo   make check            Run all checks

run:
	cd $(APP_DIR) && uv run python run.py

format:
	cd $(APP_DIR) && uv run ruff format .

lint:
	cd $(APP_DIR) && uv run ruff check .

typecheck:
	cd $(APP_DIR) && uv run mypy .

test:
	cd $(APP_DIR) && uv run pytest

check: format lint typecheck test
