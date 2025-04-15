# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.10.17

ARG INIT=true

WORKDIR /app

# install Java
RUN apt-get update && apt-get install -y default-jre

# install py packages
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app /app
COPY app/pyspark_deducer.py /app/pyspark_deducer_baked.py

# create folders
RUN mkdir -p data/input
RUN mkdir -p data/output

# deduce lookup data structures
RUN if [ '$INIT' = "true" ]; then\
        echo "Performing INIT"; \
        python init_script.py; \
    else \
        echo "Skipping INIT"; \
    fi

EXPOSE 4040

ENTRYPOINT ["python", "pyspark_deducer_baked.py"]
