FROM continuumio/miniconda3 as base

ENV PYTHONUNBUFFERRED=1

RUN apt-get update -y && apt-get install -y \
	wget \
	tar \
	tree

WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./bin ./bin
COPY ./hcad ./hcad
COPY ./tests ./tests
COPY setup.cfg .
COPY setup.py .
