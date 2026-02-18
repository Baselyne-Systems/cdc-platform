# Multi-stage build for the CDC pipeline worker image.
# Usage:
#   docker build -t cdc-platform .
#   docker run -v ./pipeline.yaml:/config/pipeline.yaml \
#              -v ./platform.yaml:/config/platform.yaml \
#              cdc-platform

# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock* ./
COPY src/ src/

# Install all optional extras so the image supports every sink type.
RUN uv sync --extra postgres --extra iceberg --no-dev --frozen 2>/dev/null \
    || uv sync --extra postgres --extra iceberg --no-dev

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM python:3.12-slim

RUN groupadd --gid 1000 cdc && \
    useradd --uid 1000 --gid cdc --create-home cdc

WORKDIR /app

# Copy the entire virtualenv from the builder.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"

# Default config mount point.
RUN mkdir -p /config && chown cdc:cdc /config
VOLUME ["/config"]

# Health probe port.
EXPOSE 8080

USER cdc

ENTRYPOINT ["cdc"]
CMD ["run", "/config/pipeline.yaml", "--platform-config", "/config/platform.yaml"]
