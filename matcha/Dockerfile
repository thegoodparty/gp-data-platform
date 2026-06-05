# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7 /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Stage 2: Runtime — same slim base, no build tools carried over
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY scripts/ /app/scripts/

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "scripts.cli"]
CMD ["--help"]
