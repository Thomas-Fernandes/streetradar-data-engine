# StreetRadar Data Engine

The engine behind [streetradar.app](https://streetradar.app) — a map of where
street-level imagery exists, across providers.

It collects **coverage**, not imagery: where panoramas exist and when they were
captured. The output is dated vector tiles, published as PMTiles.

> ### 🚧 Being rebuilt
>
> The engine is currently being rebuilt from scratch. The previous version — three
> unrelated pipelines, ~6,700 lines, last published output 11–14 months old — has
> been moved wholesale to [`archive/`](archive/README.md) and is kept as a
> reference and a regression oracle. Nothing in `archive/` runs.
>
> `src/` is where the new engine lives. It is currently a skeleton.
>
## Design documents

The audit of the previous engine, the target architecture, the migration plan and
nine decision records are maintained **outside this repository** (`docs/`, not
version-controlled). They cover, in order:

- **Audit** — what the old engine did, module by module, with measured volumes and
  20 findings ranked by severity
- **Architecture** — provider abstraction, pipeline stages, data model with capture
  dates, tile format, storage tiering, orchestration, alerting, observability, cost
  model
- **Migration** — 16 ordered steps, each independently shippable
- **ADRs** — clean break, tile format, orchestrator, status page, storage
  placement, the retired `Provider` ABC, `robots.txt` handling, enumeration
  strategy, `streetlevel` usage

## Providers

| Provider | Region | Status | Time dimension |
| --- | --- | --- | --- |
| Já 360 | Iceland | first target of the rebuild | yes — month precision |
| Apple Look Around | global | to build; no source ever existed | yes — full UTC timestamp |
| Naver | South Korea | to port | **no** — the provider's tiles carry no date |
| Mapy.cz | Czechia | 16.2M records collected, never published | yes — full date |
| Mapillary, Panoramax | open data | planned | yes |

Google, Bing and Yandex stay client-side hotlinks with **no server-side
collection**. That is a deliberate and permanent scope decision.

## Design rules that are not negotiable

These are the invariants the rebuild is organised around. Everything else is open
to discussion.

1. **One outbound path.** Every request goes through `streetradar.fetch`, which
   is the only place that knows about `robots.txt`, per-host budgets, retries and
   metrics. No proxies, no rotation, no detection evasion — rate limiting is
   politeness, rotation is evasion.
2. **Claimable, persisted work units.** Nothing is marked done before its rows
   are durable. A `kill -9` at hour 200 loses at most one shard.
3. **`decode` is pure**, does no I/O, and is tested against a recorded fixture.
4. **Nothing publishes without a finished run** plus a `verify_tiles` gate.
5. **Versioned keys and a `latest.json` pointer.** A live object is never
   overwritten.
6. **A three-level provider kill switch**, matching the front end's, before
   anything is scheduled.
7. **A bounded probe (≤ 2,000 requests) before each collector.** Enumeration
   strategy is measured, not inherited.
8. **Capture dates are first-class**, with an explicit precision so the front end
   never over-claims.

## Development

```bash
uv sync                      # core + dev: enough to run the tests
uv sync --extra providers    # adds the geospatial stack (still sized for archive/)
```

Quality gates, all blocking in CI:

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy                  # strict, zero errors — archive/ is excluded
uv run pytest -q
uv lock --check
```

`mypy` runs with `strict = true` and a zero-error baseline. That was only
possible because `archive/` is excluded — the old engine reported 58 errors
across 7 files and CI could only ratchet against the count.

## Layout

```
src/streetradar/     the engine (skeleton — the migration plan gives the order)
archive/             the previous engine — reference only, never run
tests/               arrives with the work queue, whose tests are the deliverable
flows/               Prefect deployments (not yet created)

docs/                not in version control — see "Design documents" above
data/                not in version control — raw fetches, Parquet parts,
                     work queues, tile builds; reproducible or archived to R2
```

## Related

The front end lives in a separate repository and deploys to Vercel. The contract
between the two is two JSON documents published to R2 — `latest.json` (which
archive to load per provider) and `status.json` (engine and run state, rendered
at `/status`). Keeping them separate is deliberate: a status page hosted on the
VPS is unavailable exactly when it matters.
