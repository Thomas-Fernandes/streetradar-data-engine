"""The contract every provider implements, and the data layout they share.

`ProviderPaths` replaces the guesswork the scripts do today. `find_ja360_pmtiles`
tries six candidate locations in order and returns the first hit, which means
the pipeline silently reads a stale artefact from a previous layout if one
happens to still be on disk. One convention, resolved from one root:

    data/<provider>/
        raw/         exactly as collected, never edited in place
        processed/   cleaned and normalised
        matched/     snapped to OSM, where the provider needs it
        tile/        the PMTiles archive that gets uploaded
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from .http import HttpPolicy
from .logging import get_logger
from .storage import R2Storage, UploadResult

__all__ = ["ProviderPaths", "Provider", "data_root"]

logger = get_logger(__name__)


def data_root() -> Path:
    """Root of the data tree. Override with $STREETRADAR_DATA_DIR."""
    override = os.getenv("STREETRADAR_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "data"


@dataclass(frozen=True)
class ProviderPaths:
    """Canonical locations for one provider's artefacts."""

    provider: str
    root: Path

    @classmethod
    def for_provider(cls, provider: str, root: Path | None = None) -> ProviderPaths:
        return cls(provider=provider, root=(root or data_root()) / provider)

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def processed(self) -> Path:
        return self.root / "processed"

    @property
    def matched(self) -> Path:
        return self.root / "matched"

    @property
    def tile(self) -> Path:
        return self.root / "tile"

    def pmtiles(self, filename: str | None = None) -> Path:
        """Path to the provider's PMTiles archive."""
        return self.tile / (filename or f"{self.provider}.pmtiles")

    def ensure(self) -> ProviderPaths:
        """Create every directory. Safe to call repeatedly."""
        for directory in (self.raw, self.processed, self.matched, self.tile):
            directory.mkdir(parents=True, exist_ok=True)
        return self


class Provider(ABC):
    """One coverage source, from collection through upload.

    Subclasses live in `scripts/<name>/` and implement only what is genuinely
    provider-specific. Anything shared — HTTP, R2, logging, configuration —
    comes from this package.

    `match_to_osm` is optional: `ja` snaps panoramas to the road network, while
    `naver` renders its own geometry directly.
    """

    #: Short identifier, also the directory name under `data/`.
    name: str

    #: Content type for the uploaded archive.
    content_type: str = "application/octet-stream"

    def __init__(self, paths: ProviderPaths | None = None) -> None:
        self.paths = paths or ProviderPaths.for_provider(self.name)
        self.log = get_logger(f"streetradar.{self.name}")

    def http_policy(self) -> HttpPolicy:
        """Rate limits for this provider.

        Defaults come from `SCRAPE_*`; override per provider with
        `SCRAPE_<NAME>_*`, e.g. SCRAPE_NAVER_MAX_CONCURRENCY.
        """
        return HttpPolicy.from_env(f"SCRAPE_{self.name.upper()}")

    @abstractmethod
    async def collect(self) -> None:
        """Fetch raw data into `paths.raw`."""

    @abstractmethod
    def process(self) -> None:
        """Turn `paths.raw` into normalised data in `paths.processed`."""

    def match_to_osm(self) -> None:
        """Snap geometry to the OSM road network. Optional."""
        self.log.debug("%s does not use OSM matching", self.name)

    @abstractmethod
    def generate_tiles(self) -> Path:
        """Build the PMTiles archive and return its path."""

    def upload(self, storage: R2Storage | None = None) -> UploadResult:
        """Upload the archive built by `generate_tiles`.

        Shared on purpose: this is the step the providers had duplicated almost
        verbatim.
        """
        archive = self.paths.pmtiles()
        return (storage or R2Storage()).upload(
            archive,
            key=archive.name,
            content_type=self.content_type,
            metadata={"data-source": self.name},
        )
