FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY quota ./quota
RUN pip install --no-cache-dir '.[test]'
COPY config ./config
COPY scripts ./scripts
COPY tests ./tests
ENV PYTHONUNBUFFERED=1
CMD ["gunicorn", "quota.api:create_app()", "--worker-class", "aiohttp.GunicornUVLoopWebWorker", "--workers", "8", "--bind", "0.0.0.0:8080", "--timeout", "60"]
