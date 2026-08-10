"""Upload the Naver PMTiles archive to Cloudflare R2.

Everything that is not specific to Naver lives in `streetradar.storage`.

Environment:
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET
    NAVER_PMTILES_PATH   override the archive location
    UPLOAD_KEY           override the object key (default: naver.pmtiles)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from streetradar.config import ConfigError
from streetradar.logging import get_logger, setup_logging
from streetradar.provider import ProviderPaths
from streetradar.storage import R2Storage, StorageError

logger = get_logger("streetradar.naver.upload")

DEFAULT_KEY = "naver.pmtiles"
# Kept as-is: this is what tiles.streetradar.app currently serves for this
# object. See the note in scripts/ja/upload_r2.py about unifying the two.
CONTENT_TYPE = "application/octet-stream"

# The pre-pipeline layout. Read only as a fallback, and loudly, so a stale
# archive cannot be uploaded without anyone noticing.
LEGACY_RELATIVE = Path("naver") / "usage" / "tiles" / DEFAULT_KEY


def resolve_archive() -> Path:
    """Locate the PMTiles archive, preferring the canonical layout."""
    override = os.getenv("NAVER_PMTILES_PATH")
    if override:
        path = Path(override).expanduser()
        if not path.is_file():
            raise StorageError(f"NAVER_PMTILES_PATH points at a missing file: {path}")
        return path

    paths = ProviderPaths.for_provider("naver")
    canonical = paths.pmtiles(DEFAULT_KEY)
    if canonical.is_file():
        return canonical

    legacy = paths.root.parent / LEGACY_RELATIVE
    if legacy.is_file():
        logger.warning("using the legacy archive at %s; move it to %s", legacy, canonical)
        return legacy

    return canonical


def main() -> int:
    setup_logging()

    try:
        archive = resolve_archive()
        if not archive.is_file():
            logger.error("archive not found: %s", archive)
            logger.error("generate it first with: python scripts/naver/generate_tiles.py")
            return 1

        result = R2Storage().upload(
            archive,
            key=os.getenv("UPLOAD_KEY", DEFAULT_KEY),
            content_type=CONTENT_TYPE,
            metadata={"data-source": "Naver"},
        )
    except (ConfigError, StorageError) as exc:
        logger.error("%s", exc)
        return 1

    logger.info("done: %s (%.1f MB, %.1f MB/s)", result.key, result.size_mb, result.average_mbps)
    return 0


if __name__ == "__main__":
    sys.exit(main())
