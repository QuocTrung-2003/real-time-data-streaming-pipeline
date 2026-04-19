#weather-streaming-pipeline\producer\Dockerfile
FROM python:3.11-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PYTHONPATH=/app
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group ingestion_service

COPY services/ingestion_service/ ./ingestion_service/

CMD ["uv", "run", "python", "ingestion_service/main_ingestion.py"]