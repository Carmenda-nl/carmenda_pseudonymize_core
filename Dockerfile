# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.13.6-slim

WORKDIR /source

# Install py packages
COPY requirements.txt .
RUN apt-get update && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY source /source

# Copy base_config.json to where deduce expects it
RUN cp /source/base_config.json /usr/local/lib/python3.13/site-packages/base_config.json

# Create folders
RUN mkdir -p data/input
RUN mkdir -p data/output

ENTRYPOINT ["python", "main.py"]
