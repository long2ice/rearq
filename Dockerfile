FROM python:3.9 as builder
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1
RUN mkdir -p /rearq
WORKDIR /rearq
COPY pyproject.toml poetry.lock /rearq/
ENV POETRY_VIRTUALENVS_CREATE false
RUN pip3 install poetry && poetry install --no-root -E mysql -E postgres
COPY . /rearq
RUN poetry install

FROM python:3.9-slim
WORKDIR /rearq
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /rearq /rearq
