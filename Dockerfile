FROM prefecthq/prefect:3-latest AS builder

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY flows ./flows
COPY src ./src

# Install uv
RUN pip install uv

# Create isolated environment in .venv
RUN uv sync --frozen --no-dev

COPY seed ./seed
COPY seed_credentials.py deploy.py ./

FROM python:3.12-slim

WORKDIR /app

# Copy application code
COPY --from=builder /app /app

# Copy already built virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PREFECT_LOGGING_LEVEL=INFO

# Entry point: seed credentials, deploy, and start worker
CMD bash -c "prefect work-pool create 'basic-pipe' --type process   && python seed_credentials.py && python deploy.py && prefect worker start --pool 'basic-pipe'"
