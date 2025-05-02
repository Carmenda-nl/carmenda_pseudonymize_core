# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.10.17

WORKDIR /app

# install py packages
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app /app

# create folders
RUN mkdir -p data/input
RUN mkdir -p data/output

ENTRYPOINT ["python", "polars_deduce.py"]
