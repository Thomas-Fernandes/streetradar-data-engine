# StreetRadar Data Engine

A comprehensive data collection and processing pipeline for street-level imagery from multiple mapping providers. This system collects panoramic street view data, processes it into standardized formats, and generates optimized tile sets for web applications.

## 🌍 Supported Providers

| Provider | Data Collection | Processing | OSM Matching | Tile Generation | R2 Upload |
|----------|:---------------:|:----------:|:------------:|:---------------:|:---------:|
| **JA360** (Iceland) | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Naver** (South Korea) | ✅ | ✅ | ❌ | ✅ | ✅ |
| **Mapy.cz** (Czech Republic) | ⚠️ | ✅ | ❌ | ❌ | ❌ |

## 🏗️ Architecture Overview

As of now, the architecture looks like this:

```mermaid
graph LR
    A[Raw Data Collection] --> B[Data Processing]
    B --> C{Provider Type}
    
    C -->|JA360| D[OSM Matching]
    C -->|Naver| E[Tile Generation]
    C -->|Mapy.cz| F[Data Storage]
    
    D --> G[Tile Generation]
    E --> H[R2 Upload]
    G --> H
    
    H --> I[Public Tile Server]
    F --> J[Archive Storage]
```

## 📁 Data Structure

```
data/
├── ja360/
│   ├── raw/           # Raw collected data
│   ├── processed/     # Cleaned and processed data
│   ├── matched/       # OSM-matched data
│   └── tile/          # Generated PMTiles
├── naver/
│   ├── raw/           # Session-based raw tiles
│   ├── processed/     # Processed coverage data
│   └── tile/          # Generated PMTiles
└── mapy/
    ├── raw/           # Raw panorama data
    └── processed/     # Processed data
```

## 🛠️ Development

### Code Structure

- `streetradar/` - Shared package: HTTP layer, R2 storage, config, logging
- `scripts/` - Provider-specific collection and processing
- `tests/` - Tests for the shared package

### Setup

```bash
uv sync --extra providers   # everything the provider scripts need
uv sync                     # shared package only, enough to run the tests
```

`--extra providers` pulls the geospatial stack. Two of its packages, pycairo
and pyexiv2, build from source and need `libcairo2-dev` and `libexiv2-dev`.

### Running the pipeline

Each stage is a one-shot container; see `docker-compose.yml`.

```bash
docker compose run --rm ja-collect
docker compose run --rm ja-tiles && docker compose run --rm ja-upload
```

Scheduling is host cron rather than a scheduler in the image. Airflow was
removed: three monthly pipelines did not justify a scheduler, a webserver and
a metadata database running permanently, and its dependency tree carried the
only critical security advisories in the project.

### Rate limits and proxies

Every outbound request from a ported collector goes through
`streetradar.http`, configured per provider with `SCRAPE_<PROVIDER>_*`:

```bash
SCRAPE_NAVER_MAX_CONCURRENCY=32
SCRAPE_NAVER_REQUESTS_PER_HOUR=20000
SCRAPE_NAVER_PROXIES=http://user:pass@host:port,http://...
```

Naver routes through it fully. Já only partially — `streetlevel` issues its own
requests, so it gets the budget and concurrency ceiling but not retries,
backoff or proxy rotation.