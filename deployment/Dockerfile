# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.13.6-slim

WORKDIR /app

# Install py packages
COPY ./app/requirements.txt .
RUN apt-get update && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./app /app
COPY ./deployment/docker-entrypoint.sh /app/

# Copy base_config.json to where deduce expects it
RUN cp /app/base_config.json /usr/local/lib/python3.13/site-packages/base_config.json

# Create folders for both mount options
RUN mkdir -p /app/data/input
RUN mkdir -p /app/data/output


# Make entrypoint script executable
RUN chmod +x /app/docker-entrypoint.sh

# Set environment variable to indicate Docker environment
ENV DOCKER_ENV=true

ENTRYPOINT ["/app/docker-entrypoint.sh"]
