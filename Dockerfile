# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

FROM python:3.13.3

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /code

RUN apt-get update \
&& apt-get install -y --no-install-recommends \
gcc \
build-essential \
libssl-dev \
libffi-dev \
python3-dev \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

COPY code/requirements.txt /code/
RUN pip install --upgrade pip \
&& pip install -r requirements.txt

COPY code /code
COPY external /external

# include the external core to services
RUN rm -rf /code/services && \
    cp -r /external/core/app /code/services && \
    rm -rf /external

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
