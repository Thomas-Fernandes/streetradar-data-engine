# The engine image: Python runtime + tippecanoe + pmtiles, one artefact (ADR 0010).
#
# Both tiling binaries are baked in — tippecanoe is not packaged for Debian, and
# building it by hand on the executing machine is exactly the undocumented manual
# step ADR 0010 exists to forbid. Every pin below matches the versions the test
# suite ran against on the dev machine, so "tests pass" speaks for these bytes.
#
# Build from the repository root (the compose file does):
#     docker build -f deploy/Dockerfile .

# --- Stage 1: the tiling binaries ------------------------------------------
# Same Debian generation as the runtime image, so the glibc they link matches.
FROM debian:bookworm-slim AS tools

ARG TIPPECANOE_VERSION=2.49.0
ARG PMTILES_VERSION=1.28.0

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git libsqlite3-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${TIPPECANOE_VERSION} \
        https://github.com/felt/tippecanoe.git /src/tippecanoe \
    && make -C /src/tippecanoe -j"$(nproc)" \
    && make -C /src/tippecanoe install PREFIX=/opt/tippecanoe

# go-pmtiles ships a static binary; the tarball holds it at its root as `pmtiles`.
RUN curl -fsSL "https://github.com/protomaps/go-pmtiles/releases/download/v${PMTILES_VERSION}/go-pmtiles_${PMTILES_VERSION}_Linux_x86_64.tar.gz" \
        | tar -xz -C /opt pmtiles

# --- Stage 2: the runtime ----------------------------------------------------
FROM python:3.12-slim-bookworm

# uv, pinned to the version the lockfile is maintained with.
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /usr/local/bin/

# tippecanoe links libsqlite3 and zlib; declared explicitly rather than relied
# on as a side effect of the Python base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsqlite3-0 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tools /opt/tippecanoe/bin/tippecanoe /usr/local/bin/
COPY --from=tools /opt/pmtiles /usr/local/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

# Dependencies first, project second: the dependency layer only rebuilds when
# the lockfile changes, not on every source edit.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH" \
    STREETRADAR_DATA_DIR=/data

# No ENTRYPOINT: the compose file names each service's command (server, runner),
# and `init: true` there provides the signal-forwarding PID 1.
