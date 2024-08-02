FROM python:3.12 AS base

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

FROM base AS dev

COPY requirements-dev.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt -r requirements-dev.txt
