FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install đúng group
RUN uv sync --no-dev --group ingestion_service

# Copy code
COPY services/metrics_consumer/ ./metrics_consumer/

# Run
CMD ["uv", "run", "python", "metrics_consumer/main_metrics.py"]