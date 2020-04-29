FROM continuumio/miniconda3

ARG PYTHON_VERSION=3.6
RUN conda install python=${PYTHON_VERSION} pytest wget

RUN wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && chmod +x wait-for-it.sh

WORKDIR aiopeewee
