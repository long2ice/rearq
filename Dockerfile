FROM python:3
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1
RUN mkdir -p /rearq
WORKDIR /rearq
COPY pyproject.toml poetry.lock /rearq/
RUN pip3 install poetry
ENV POETRY_VIRTUALENVS_CREATE false
RUN poetry install --no-root -E mysql -E postgres
COPY . /rearq
RUN poetry install
