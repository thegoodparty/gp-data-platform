# Stage 1: Build dependencies
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first, so a source-only change reuses this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ /app/src/
# --no-editable so the venv is self-contained: stage 2 copies /app/.venv alone and
# the `hsm` console script resolves without /app/src coming with it.
RUN uv sync --frozen --no-dev --no-editable

# Stage 2: runtime, with no build tools
FROM python:3.14-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --uid 10001 --no-create-home hsm
USER hsm

ENTRYPOINT ["hsm"]
CMD ["--help"]
