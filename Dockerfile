FROM continuumio/miniconda3 as base

ENV PYTHONUNBUFFERRED=1

WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

