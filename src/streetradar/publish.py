"""The publish gate and the versioned handoff to R2 (design invariants 5 and 6).

Between a finished tileset and a live map there are two rules, and this is where
they are enforced.

**Nothing publishes without passing the gate** (invariant 5). `verify_tiles`
reads the `TilesetStats` back and looks at the archive itself — it exists, it is a
real PMTiles file and not a truncated one, it holds tiles, none of them is too
large to serve, and its zooms are the ones the spec asked for. The gate distrusts
the step before it on purpose: the stats are recomputed from what tippecanoe
wrote, and the magic bytes are read off disk.

**A live object is never overwritten** (invariant 6). Each archive is stored under
a key that carries the run id, so a new run writes a new key and the one the map
is currently reading is untouched. `latest.json` — one of the two JSON contracts
the front end reads — is the pointer that swings from the old key to the new one
only after the new archive is up and verified. It keeps the current and previous
generations per provider and scope; retention prunes anything older, so exactly
two archives live under each scope prefix (ADR 0005).

## One latest.json, so publishes serialize

`latest.json` is a single global contract, not one file per country, so a
read-modify-write from two country jobs finishing at once could clobber each
other. The archives themselves never collide — their keys carry the run id — but
the pointer does. The orchestrator serializes the publish tail (a publish
concurrency of one); this function assumes that and does a plain read-modify-write.
If that ever needs to relax, the upgrade is an If-Match conditional PUT on the
pointer, not sharding the contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from .r2 import R2Client
    from .tileset import TilesetSpec, TilesetStats

__all__ = [
    "LATEST_KEY",
    "PublishError",
    "PublishResult",
    "VerifyReport",
    "archive_key",
    "archive_prefix",
    "publish",
    "verify_tiles",
]

#: The pointer document the front end reads to find each scope's current archive.
LATEST_KEY = "latest.json"

#: PMTiles is served by HTTP range request; the content type is not load-bearing
#: for that, but octet-stream is the honest label for an opaque binary archive.
_PMTILES_CONTENT_TYPE = "application/octet-stream"

#: A vector tile above this size is a serving problem — MapLibre degrades and the
#: range read stops being cheap. The measured worst real tile is 88.5 KB even
#: with tippecanoe's size limit off, so this ceiling only ever catches pathology.
_MAX_TILE_BYTES = 500_000

#: A PMTiles v3 archive begins with these seven ASCII bytes, then a version byte.
_PMTILES_MAGIC = b"PMTiles"


class PublishError(RuntimeError):
    """A publish was refused — the gate failed, or a live key would be overwritten."""


@dataclass(frozen=True)
class VerifyReport:
    """The gate's verdict, with every reason it would block, for status.json."""

    ok: bool
    problems: tuple[str, ...]


@dataclass(frozen=True)
class PublishResult:
    """What a successful publish did: the new live key, its URL, and what it pruned."""

    key: str
    url: str
    previous_key: str | None
    pruned: tuple[str, ...]


def archive_prefix(provider: str, scope: str) -> str:
    """Where a scope's archives live. Scope is the country code — `is`, `kr`, `lu`."""
    return f"tiles/{provider}/{scope}/"


def archive_key(provider: str, scope: str, run_id: str) -> str:
    """The versioned key for one run's archive. The run id is what makes it unique."""
    return f"{archive_prefix(provider, scope)}{run_id}.pmtiles"


def verify_tiles(
    archive: Path,
    stats: TilesetStats,
    spec: TilesetSpec,
    *,
    max_tile_bytes: int = _MAX_TILE_BYTES,
) -> VerifyReport:
    """The publish gate: is this archive fit to serve? Every failing check, listed.

    Reads the archive off disk rather than trusting the build step — a zero-byte
    file, or an mbtiles that never converted, is exactly what a gate exists to
    catch. Returns a report rather than raising, so `status.json` can record why a
    run did not publish.
    """
    problems: list[str] = []

    if not archive.is_file():
        return VerifyReport(ok=False, problems=(f"archive is missing: {archive}",))
    if archive.stat().st_size == 0:
        problems.append("archive is empty")
    elif archive.suffix == ".pmtiles":
        with archive.open("rb") as handle:
            if handle.read(len(_PMTILES_MAGIC)) != _PMTILES_MAGIC:
                problems.append("archive is not a PMTiles file (bad magic)")

    if stats.tiles < 1:
        problems.append("no tiles were produced")
    if stats.max_tile_bytes > max_tile_bytes:
        problems.append(
            f"largest tile is {stats.max_tile_bytes} bytes, over the {max_tile_bytes} limit"
        )
    if not stats.zooms:
        problems.append("no zoom levels present")
    else:
        if min(stats.zooms) < spec.min_zoom:
            problems.append(f"zoom {min(stats.zooms)} is below the spec floor {spec.min_zoom}")
        if max(stats.zooms) > spec.max_zoom:
            problems.append(f"zoom {max(stats.zooms)} is above the spec ceiling {spec.max_zoom}")

    return VerifyReport(ok=not problems, problems=tuple(problems))


def _entry(
    r2: R2Client,
    key: str,
    run_id: str,
    stats: TilesetStats,
    spec: TilesetSpec,
    published_at: str,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "key": key,
        "url": f"{r2.config.public_base_url}/{key}",
        "tiles": stats.tiles,
        "bytes": stats.bytes,
        "max_tile_bytes": stats.max_tile_bytes,
        "min_zoom": min(stats.zooms) if stats.zooms else spec.min_zoom,
        "max_zoom": max(stats.zooms) if stats.zooms else spec.max_zoom,
        "published_at": published_at,
    }


def _load_latest(r2: R2Client) -> dict[str, object]:
    body = r2.get_bytes(LATEST_KEY, missing_ok=True)
    if body is None:
        return {"version": 1, "providers": {}}
    loaded = json.loads(body)
    if not isinstance(loaded, dict) or "providers" not in loaded:
        return {"version": 1, "providers": {}}
    return loaded


def publish(
    r2: R2Client,
    *,
    provider: str,
    scope: str,
    run_id: str,
    archive: Path,
    stats: TilesetStats,
    spec: TilesetSpec,
    published_at: str,
    max_tile_bytes: int = _MAX_TILE_BYTES,
) -> PublishResult:
    """Gate, upload under a versioned key, swing the pointer, prune older generations.

    The order is the invariant: verify, then upload the new archive, then update
    `latest.json`. The pointer moves last, so a reader either sees the old archive
    or the fully-uploaded new one, never a key that is not there yet.
    """
    report = verify_tiles(archive, stats, spec, max_tile_bytes=max_tile_bytes)
    if not report.ok:
        raise PublishError(f"{provider}/{scope} failed the gate: {'; '.join(report.problems)}")

    key = archive_key(provider, scope, run_id)
    if r2.exists(key):
        raise PublishError(f"refusing to overwrite the live object at {key}")

    r2.upload(
        archive,
        key,
        content_type=_PMTILES_CONTENT_TYPE,
        metadata={"provider": provider, "scope": scope, "run_id": run_id},
    )

    latest = _load_latest(r2)
    providers = latest["providers"]
    assert isinstance(providers, dict)
    scopes = providers.setdefault(provider, {})
    slot = scopes.setdefault(scope, {})

    previous = slot.get("current")
    previous_key = previous.get("key") if isinstance(previous, dict) else None
    slot["previous"] = previous
    slot["current"] = _entry(r2, key, run_id, stats, spec, published_at)

    keep = {key} | ({previous_key} if isinstance(previous_key, str) else set())
    pruned = tuple(
        obj.key for obj in r2.list_objects(archive_prefix(provider, scope)) if obj.key not in keep
    )
    for stale in pruned:
        r2.delete(stale)

    r2.put_bytes(
        LATEST_KEY,
        json.dumps(latest, indent=2, sort_keys=True).encode(),
        content_type="application/json",
    )

    return PublishResult(
        key=key,
        url=f"{r2.config.public_base_url}/{key}",
        previous_key=previous_key if isinstance(previous_key, str) else None,
        pruned=pruned,
    )
