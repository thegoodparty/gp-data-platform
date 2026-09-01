# Stage 1: Build dependencies
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7 /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Stage 2: Runtime — same slim base, no build tools carried over
FROM python:3.14-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY shared/ /app/shared/
COPY stitch_golden_data/ /app/stitch_golden_data/
COPY bedrock_clients/ /app/bedrock_clients/

# The image carries no .git, so run provenance comes from this env; the daily
# entry point refuses to run with it unset.
ARG GIT_SHA=""
ENV GIT_SHA=$GIT_SHA

ENV PATH="/app/.venv/bin:$PATH"

# Two entry points share this image (the supervised backlog run and the daily
# run), so the module is an argument rather than baked in.
ENTRYPOINT ["python", "-m"]
CMD ["stitch_golden_data.prod_gold_data.backlog_run", "--help"]
