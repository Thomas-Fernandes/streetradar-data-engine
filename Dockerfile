# Runtime image for the collection pipelines.
#
# Two stages so the compiler toolchain needed by pycairo and pyexiv2 does not
# ship to the VPS. Only the built virtualenv crosses over.

# ---------------------------------------------------------------- build ----
FROM python:3.12-slim-bookworm AS build

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# pycairo and pyexiv2 build from source; the rest resolve to wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libcairo2-dev \
        libexiv2-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies resolve from the lockfile alone, so this layer is only rebuilt
# when pyproject.toml or uv.lock actually change.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY streetradar/ ./streetradar/
COPY scripts/ ./scripts/
RUN uv sync --frozen --no-dev

# -------------------------------------------------------------- runtime ----
FROM python:3.12-slim-bookworm AS runtime

# Runtime shared objects only — no headers, no compiler.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libcairo2 \
        libexiv2-27 \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 pipeline

WORKDIR /app

COPY --from=build --chown=pipeline:pipeline /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREETRADAR_DATA_DIR=/data

# Collections run for hours and write GBs. The volume is what survives a
# container restart, and what makes checkpointing worth anything.
VOLUME ["/data"]

USER pipeline

# tini reaps zombies and forwards SIGTERM, so `docker stop` actually stops a
# long collection rather than waiting out the timeout and killing it.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "import streetradar; print('streetradar', streetradar.__version__)"]
