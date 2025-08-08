# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.21-alpine

WORKDIR /source

# install py packages
COPY requirements.txt .
RUN apk update && apk upgrade && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apk cache clean

COPY source /source

# create folders
RUN mkdir -p data/input
RUN mkdir -p data/output

ENTRYPOINT ["python", "main.py"]
