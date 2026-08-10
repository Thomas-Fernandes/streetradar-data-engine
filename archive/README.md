# `archive/` — the previous engine

This directory holds the StreetRadar engine as it existed at commit `33daa04`
(2026-08-10), before the rebuild. It was moved here wholesale, unmodified, by the
decision recorded in `docs/adr/0001-clean-break.md`.

**Nothing here runs, is scheduled, or gets fixed.** This is not dead code awaiting
cleanup — it is a version-controlled reference with a specific value, described
below.

## What is in here

```
archive/
  scripts/ja/       5 files, 3,027 lines  — the only complete pipeline
  scripts/naver/    5 files, 3,020 lines  — the largest dataset
  scripts/mapy/     2 files,   658 lines  — never shipped, best resume support
  streetradar/      6 files,   926 lines  — the shared package
  tests/            4 files,   442 lines  — 48 tests, all passing
  Dockerfile                              — two-stage build, pycairo + pyexiv2
  .dockerignore
  docker-compose.yml                      — host-cron orchestration
```

`Dockerfile` and `.dockerignore` are here because they reference `scripts/` and
`streetradar/`: leaving them at the root would have produced a broken image. The
new `Dockerfile` is written against the new stack — no `pycairo`, no `pyexiv2`,
plus `tippecanoe` and `pmtiles`.

Deleted and **not** archived, because they carry no reference value:

| Deleted | Why |
| --- | --- |
| `proxy.py` | Five commercial proxy endpoints with plaintext credentials. **Treat those credentials as compromised and revoke them.** Imported by nothing — `ProxyPool` read the environment, not this file. |
| `airflow/` | 1.8 MB of example-DAG logs and a SQLite database, runtime residue of the Airflow removal in `40890f8`. Untracked, with no project-specific content. |
| `venv/` | A stale 1.6 GB virtualenv (September 2025). |
| `scripts/yandex/` | An empty `captures/` directory. Yandex stays a client-side hotlink with no server-side collection. |
| `.mypy-baseline` | The CI ratchet has no purpose now: `archive/` is excluded from typechecking, so `src/` starts at zero errors under `strict`. |

## What this is still good for

Three legitimate uses.

### 1. Regression oracle

`scripts/ja/match_to_osm.py` (1,172 lines) is real map-matching: local edge
bearing at the snap position, rejection beyond 135° of heading deviation, coverage
intervals at 12 m, A–D grading, dissolution of adjacent segments. Its output is
recorded in `data/ja360/usage/stats.json`:

```
798,823 panoramas · 625,934 snapped (78.4%)
29,715 covered roads · 10,693.1 / 27,262.2 km  →  59.6%
95,618 segments
quality  D 354,656 · C 142,416 · B 81,655 · A 47,207
parameters  radius 25 m · max snap 30 m · max heading deviation 135° · coverage 12 m
```

The rewrite of that stage (`docs/MIGRATION.md` step 8) must reproduce these
figures to within a few tenths of a point. **When they diverge, this is where you
come to find out why** — bearing conventions and interval-merging edge cases are
exactly what gets rewritten "almost" correctly.

### 2. Protocol and constant reference

Hard-won values that cannot be re-derived by reasoning:

| Where | What |
| --- | --- |
| `scripts/naver/boundings.geojson` | the Korea collection polygon |
| `scripts/ja/collect_data.py:29-51` | the three Icelandic zone seeds (Reykjavík, Vestmannaeyjar, Grímsey) and their search radii |
| `scripts/naver/process_data.py` | MVT decoding of Naver tiles, and preservation of `properties` |
| `scripts/*/generate_tiles.py` | the per-zoom style tables (line widths, halos, `#fbbe01` for Já, `#03cf5d` for Naver) — the rendering spec to reproduce client-side in MVT |
| `streetradar/http.py:195-202` | why a sliding window rather than a token bucket |
| `streetradar/http.py:354-359` | why backoff jitter is necessary |

Read the last two **before** rewriting the `fetch/` layer (`docs/MIGRATION.md`
step 4). The reasoning there is correct and documented; it is the kind of detail
that gets reintroduced wrongly when redone from memory.

### 3. Archaeology

Why a given constant has a given value, what has already been tried, and what
failed. `git log` on this directory reaches the full history from before the break.

## What this is not for

- **Do not run it.** Several scripts are broken as they stand: `naver-collect`
  raises `FileNotFoundError` from `/app` (a CWD-relative path), the `pmtiles` CLI
  is not in the image, and `import streetlevel.lookaround` fails for want of
  `torchvision`.
- **Do not fix it.** A bug found here is noted in `docs/AUDIT.md` if it is not
  already there, and fixed in `src/`.
- **Do not include it in linting or typechecking.** `pyproject.toml` excludes
  `archive/` from `mypy` and `ruff` — that is what took the baseline from 58 errors
  to 0 and lets new code start under `--strict`.
- **Do not import from it in `src/`.** If an archived file deserves to come back,
  it comes back in a dedicated commit that says so explicitly. The default
  assumption is a rewrite.

## What has been promoted out of here

Nothing so far. Keep this section current: each promotion is a dedicated commit,
recorded here with its reason.

| Date | File | To | Reason |
| --- | --- | --- | --- |
| — | — | — | — |

## When to delete this directory

Once all three conditions hold:

1. all five providers are ported to the new core and publishing;
2. the `osm_match` figures are pinned by a regression test that no longer depends
   on reading the old code;
3. the constants in the table above have been copied into `src/` with their
   provenance in a comment.

Until then it costs nothing: it is excluded from linting, typechecking and the
Docker image, and it weighs under 800 KB.

## Full context

The design documents are **not** version-controlled in this repository (`docs/` is
in `.gitignore`). If you are reading this from a clone, they are not here.

- `docs/AUDIT.md` — what this code did, module by module, with measured defects and
  20 findings ranked by severity
- `docs/ARCHITECTURE.md` — what replaces it
- `docs/MIGRATION.md` — in what order
- `docs/adr/0001-clean-break.md` — why everything is here rather than ported
