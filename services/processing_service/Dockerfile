FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

USER root

# Install basic tools
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


WORKDIR /app


# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev --group processing_service

# Copy source code
COPY services/processing_service/ ./processing_service/

# Environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYTHONPATH="/app:/app/.venv/lib/python3.11/site-packages"

# Run app
CMD ["uv", "run", "python", "processing_service/main_processing.py"]