# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install py packages
COPY ./app/pyproject.toml ./app/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY ./app /app

# Create folders for both mount options
RUN mkdir -p /app/data/input
RUN mkdir -p /app/data/output

# Set environment variable to indicate Docker environment
ENV DOCKER_ENV=true

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
