FROM python:3.11-slim

ENV PYTHONPATH=/app


WORKDIR /app

# -------------------------------
# 1. Install uv
# -------------------------------
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
# -------------------------------
# 2. Copy dependency files
# -------------------------------
COPY pyproject.toml uv.lock ./

# -------------------------------
# 3. Install dependencies
# -------------------------------
RUN uv sync --no-dev --group api_service

# -------------------------------
# 4. Copy source code
# -------------------------------
COPY services/api_service/ ./api_service/

# -------------------------------
# 5. Run API
# -------------------------------
CMD ["uv", "run", "uvicorn", "api_service.main_api:app", "--host", "0.0.0.0", "--port", "8000"]




