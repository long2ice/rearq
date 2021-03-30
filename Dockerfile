FROM python:3
RUN mkdir -p /rearq
WORKDIR /rearq
COPY pyproject.toml poetry.lock /rearq/
RUN pip3 install poetry
ENV POETRY_VIRTUALENVS_CREATE false
RUN poetry install --no-root
COPY . /rearq
RUN poetry install
