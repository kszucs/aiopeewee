FROM continuumio/miniconda3

ARG PYTHON_VERSION=3.6
RUN conda install python=${PYTHON_VERSION} pytest
