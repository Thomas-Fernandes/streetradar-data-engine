"""Writing observations out as immutable parquet parts.

The VPS has 150 GB of disk and a world-scale collection does not fit on it, so
nothing accumulates locally: a part is written, handed off, and deleted. Steady
state stays under a couple of gigabytes regardless of how large the country is.

Two properties make that safe.

**Parts are immutable.** Once written a part never changes, so uploading it and
deleting the local copy needs no coordination. A part that was being written when
the process died is a `.tmp` file nothing references, and it is discarded.

**The caller controls the ordering.** `flush` returns the path it wrote; the
caller uploads it, and only then snapshots the collection state. Snapshotting
first is what turns a crash into silently missing rows — the failure the previous
engine had, where a panorama id was marked visited before its fetch succeeded and
was then unrecoverable.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from .model import CapturePrecision

__all__ = ["Batch", "PartWriter", "write_part"]


class Batch(Protocol):
    """What a writer needs of whatever it is accumulating.

    Coverage arrives in two shapes — panorama positions from Apple, road lines
    from Já and Naver — and they do not share a schema. They do share how they
    are made durable, so the writer knows only this much about them.
    """

    provider: str
    run_id: str
    capture_precision: CapturePrecision

    def __len__(self) -> int: ...
    def to_arrow(self, *, sort: bool = True) -> pa.Table: ...
    def clear(self) -> None: ...


#: zstd level 9 rather than snappy: measured at 8.0 bytes per row against 17.0,
#: for compression CPU that is irrelevant next to the available headroom —
#: decoding alone runs at 14,900 tiles/s per core and a collection needs 1,500.
_COMPRESSION = "zstd"
_COMPRESSION_LEVEL = 9

#: Row groups large enough that delta encoding has a long run to work with, small
#: enough that a reader can skip most of a part using statistics.
_ROW_GROUP_SIZE = 1_000_000


def write_part(table: pa.Table, path: Path) -> Path:
    """Write one part atomically, returning its path."""
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(
        table,
        tmp,
        compression=_COMPRESSION,
        compression_level=_COMPRESSION_LEVEL,
        row_group_size=_ROW_GROUP_SIZE,
        write_statistics=True,
    )
    tmp.replace(path)
    return path


class PartWriter[B: Batch]:
    """Accumulates observations and writes a part once enough have arrived.

    Sized by row count rather than bytes: the compressed size of a part is not
    known until it is written, and at a measured 8 bytes per observation or 16
    per line the count is a good enough proxy.

    The batch is passed in rather than built here, because what a row *is*
    differs by provider while making it durable does not. It is generic over the
    batch so that `writer.batch` keeps the concrete type — the writer needs only
    the `Batch` protocol, but a caller filling it needs its real `add`.
    """

    def __init__(
        self,
        directory: Path,
        batch: B,
        *,
        rows_per_part: int = 20_000_000,
    ) -> None:
        if rows_per_part < 1:
            raise ValueError("rows_per_part must be positive")
        self.directory = directory
        self.batch: B = batch
        self.provider = batch.provider
        self.run_id = batch.run_id
        self.rows_per_part = rows_per_part
        self.parts_written = 0
        self.rows_written = 0

    @property
    def pending(self) -> int:
        """Rows buffered but not yet in a part — lost if the process dies."""
        return len(self.batch)

    def full(self) -> bool:
        return len(self.batch) >= self.rows_per_part

    def flush(self) -> Path | None:
        """Write the buffered rows as a part. Returns None when there are none.

        Upload the returned path **before** snapshotting collection state: the
        tiles in a part that was never uploaded must be re-probed, and that only
        happens if the state does not yet claim them.
        """
        if not len(self.batch):
            return None
        table = self.batch.to_arrow()
        path = self.directory / f"{self.provider}-{self.run_id}-{self.parts_written:05d}.parquet"
        write_part(table, path)
        self.parts_written += 1
        self.rows_written += table.num_rows
        self.batch.clear()
        return path
