"""Upload the Já 360 PMTiles archive to Cloudflare R2.

Everything that is not specific to Já — the R2 client, hashing, progress
reporting, verification — lives in `streetradar.storage`. This file is left
with the two things that genuinely differ between providers: where the archive
is and how it should be labelled.

Environment:
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET
    JA360_PMTILES_PATH   override the archive location
    UPLOAD_KEY           override the object key (default: ja360.pmtiles)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from streetradar.config import ConfigError
from streetradar.logging import get_logger, setup_logging
from streetradar.provider import ProviderPaths
from streetradar.storage import R2Storage, StorageError

logger = get_logger("streetradar.ja.upload")

DEFAULT_KEY = "ja360.pmtiles"
# Kept as-is: this is what tiles.streetradar.app currently serves for this
# object, and naver uses application/octet-stream for the same kind of archive.
# Worth unifying, but not silently as part of a refactor.
CONTENT_TYPE = "application/x-protobuf"


def resolve_archive() -> Path:
    """Locate the PMTiles archive.

    The explicit override wins; otherwise the canonical layout is the only
    place we look. The previous version searched six candidate locations in
    order, which meant a leftover file from an older layout could be uploaded
    in place of a freshly generated one.
    """
    override = os.getenv("JA360_PMTILES_PATH")
    if override:
        path = Path(override).expanduser()
        if not path.is_file():
            raise StorageError(f"JA360_PMTILES_PATH points at a missing file: {path}")
        return path

    return ProviderPaths.for_provider("ja360").pmtiles(DEFAULT_KEY)


def main() -> int:
    setup_logging()

    try:
        archive = resolve_archive()
        if not archive.is_file():
            logger.error("archive not found: %s", archive)
            logger.error("generate it first with: python scripts/ja/generate_tiles.py")
            return 1

        result = R2Storage().upload(
            archive,
            key=os.getenv("UPLOAD_KEY", DEFAULT_KEY),
            content_type=CONTENT_TYPE,
            metadata={
                "data-source": "JA360",
                "tile-type": "raster-coverage",
                "zoom-range": "1-16",
            },
        )
    except (ConfigError, StorageError) as exc:
        logger.error("%s", exc)
        return 1

    logger.info("done: %s (%.1f MB, %.1f MB/s)", result.key, result.size_mb, result.average_mbps)
    return 0


if __name__ == "__main__":
    sys.exit(main())
