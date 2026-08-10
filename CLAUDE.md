# CLAUDE.md

Project conventions for the StreetRadar data engine. Loaded automatically each
session.

## Never generate scraping traffic

This is the one rule with legal consequences, so it comes first.

- **Never run a collector, a probe, or anything that issues a request to a
  provider.** Reading code, running `ruff`/`mypy`/`pytest`, and inspecting local
  files are all fine.
- `ENGINE_ALLOW_OUTBOUND` defaults to `0` and is `1` only in the VPS environment.
  Do not set it locally, and do not add a code path that bypasses it.
- Bounded enumeration probes (≤ 2,000 requests) run **on the VPS**, never from a
  dev machine: one outbound identity, one budget.
- No proxies, no rotation, no detection evasion, ever. Rate limiting is
  politeness; rotation is evasion. See `docs/adr/0007-robots-handling.md`.
- Fetching a host's `robots.txt` is not scraping — it is the compliance
  mechanism, and one request per host is expected.

## Language: English

All of it, without exception: source, comments, docstrings, commit messages, PR
descriptions, documentation, ADRs, and the two JSON contracts published to R2.

The project is a public technical showcase and an ADR's stated purpose is to be
read by a future reader of the repository. Conversation with the maintainer may be
in French; **artefacts committed or written to disk are English.**

## `archive/` is read-only

`archive/` holds the previous engine, moved there wholesale (see
`docs/adr/0001-clean-break.md`). It is excluded from `ruff`, `mypy` and `pytest`.

- **Never run it.** Several scripts are broken by design after the move.
- **Never fix it.** A bug found there is noted in `docs/AUDIT.md` if it is not
  already, and fixed in `src/`.
- **Never import from it** into `src/`. If an archived file deserves to come back,
  it comes back in a dedicated commit that says so.
- **Do read it.** It is the regression oracle for `osm_match` and the reference
  for hard-won constants. See `archive/README.md`.

## `docs/` is not version-controlled

`docs/` is in `.gitignore` by the maintainer's decision. It is the only copy of
the audit, architecture, migration plan and ten ADRs — do not assume `git` will
recover it, and do not be surprised that it never appears in `git status`.

## Quality gates

All four block in CI. Run them before claiming work is done:

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy          # no arguments: `mypy .` would override `files` and walk archive/
uv run pytest -q     # exit 5 ("no tests collected") is the correct state for now
uv lock --check
```

`mypy` runs with `strict = true` at **zero errors**. Do not add `# type: ignore`
or per-module `ignore_errors` to get past a failure — the zero baseline was
expensive to obtain (the old engine had 58 errors across 7 files and CI could only
ratchet against the count). Missing third-party stubs are handled by an explicit
`[[tool.mypy.overrides]]` entry, so a new untyped dependency is a visible decision.

## Design invariants

Do not silently violate these; they are what the architecture is organised
around. Full detail in `docs/ARCHITECTURE.md`, the reasoning in `docs/adr/`.

1. **One outbound path.** Every request goes through `streetradar.fetch`, the only
   place that knows about `robots.txt`, per-host budgets, retries and metrics.
   Calling `aiohttp`/`requests` directly, or a `streetlevel` `find_panorama_*`
   wrapper, silently opts out of all of them.
2. **`streetlevel` is used via its public `build_*_url` and `parse_*` functions,
   never its `find_panorama_*` wrappers.** ADR 0009.
3. **Claimable, persisted work units.** Nothing is marked done before its rows are
   durable. Never mark a unit visited before its fetch succeeds.
4. **`decode` is pure**, does no I/O, and is tested against a recorded fixture.
5. **Nothing publishes without a finished run plus a `verify_tiles` gate.**
6. **Versioned keys and a `latest.json` pointer.** A live object is never
   overwritten.
7. **Capture dates carry an explicit `capture_precision`**, so the front end never
   over-claims (Já is month-precision; Apple is a full UTC timestamp).
8. **A bounded probe precedes each collector.** Enumeration strategy is measured,
   not inherited — this is what caught Já re-fetching neighbour records it already
   held.

## Deployment

Artefact is a Docker image built in CI and published to GHCR on git tag; the VPS
pulls it. **CI never enters the VPS** — no SSH key in CI secrets. Deploying during
a multi-week collection is safe because shards are bounded and idempotent, not
because of deployment tooling. ADR 0010.

## Where things are

```
src/streetradar/   the engine
archive/           the previous engine — reference only
docs/              design documents (not version-controlled)
data/              not version-controlled: raw fetches, Parquet, work queues, tiles
deploy/            Dockerfile, compose, Ansible playbook, migrations
```
