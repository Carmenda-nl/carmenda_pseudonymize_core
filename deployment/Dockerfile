# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.13.6-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install py packages
COPY ./app/requirements.txt .
RUN apt-get update && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./app /app

# Create folders for both mount options
RUN mkdir -p /app/data/input
RUN mkdir -p /app/data/output

# Set environment variable to indicate Docker environment
ENV DOCKER_ENV=true

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
