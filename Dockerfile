FROM continuumio/miniconda3

ARG PYTHON_VERSION=3.6
RUN conda install -c conda-forge \
        python=${PYTHON_VERSION} \
        pytest \
        wget

RUN pip install "peewee<3.0" aiomysql pytest-asyncio==0.10.0

RUN wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && chmod +x wait-for-it.sh

WORKDIR aiopeewee
