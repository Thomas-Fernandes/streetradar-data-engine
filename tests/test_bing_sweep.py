"""The certified-disc sweep.

Driven against a synthetic point field with a controllable endpoint model, so
the two properties that matter can be checked directly: that a sweep over a
FAITHFUL endpoint recovers every point, and that the AUDIT catches an endpoint
that drops points inside the radius it is trusted to cover.

The synthetic probe mimics the real one — return the nearest `cap` points to the
query centre — with an optional loss model that drops points beyond a fraction of
the returned radius, which is the rim loss measured on the real endpoint.
"""

from __future__ import annotations

import math

import pytest

from streetradar.bing import CERTIFY_FRACTION, Bubble
from streetradar.bing_sweep import Cell, Result, audit, certified_radius, sweep

CAP = 2048


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


def grid_field(n: int, spacing_deg: float, lat0: float, lon0: float) -> list[Bubble]:
    """An n x n lattice of bubbles — a stand-in for a dense covered area."""
    field = []
    k = 0
    for i in range(n):
        for j in range(n):
            k += 1
            field.append(
                Bubble(
                    id=k,
                    lat=lat0 + i * spacing_deg,
                    lon=lon0 + j * spacing_deg,
                    captured_at=None,
                    prev=k - 1 if j else None,
                    next=k + 1 if j < n - 1 else None,
                )
            )
    return field


class Endpoint:
    """A synthetic StreetSideBubbleMetaData: nearest `cap`, optional rim loss.

    `loss_beyond` drops a fraction of the points that lie beyond that fraction of
    the returned radius, reproducing the measured behaviour where omissions
    concentrate near the rim. loss_beyond=1.0 is faithful (no loss).
    """

    def __init__(self, field: list[Bubble], *, loss_beyond: float = 1.0, drop: float = 0.5) -> None:
        self.field = field
        self.loss_beyond = loss_beyond
        self.drop = drop
        self.calls = 0

    async def probe(self, lat: float, lon: float) -> list[Bubble]:
        self.calls += 1
        ranked = sorted(self.field, key=lambda b: haversine(lat, lon, b.lat, b.lon))
        near = ranked[:CAP]
        if self.loss_beyond >= 1.0 or not near:
            return near
        r = haversine(lat, lon, near[-1].lat, near[-1].lon)
        threshold = self.loss_beyond * r
        kept = []
        for idx, b in enumerate(near):
            d = haversine(lat, lon, b.lat, b.lon)
            # Deterministic thinning beyond the threshold: drop every other one.
            if d > threshold and idx % 2 == 0:
                continue
            kept.append(b)
        return kept


BOUNDS = (-0.14, 51.50, -0.11, 51.52)  # west, south, east, north


class TestCell:
    def test_a_cell_splits_into_four_covering_quarters(self) -> None:
        cell = Cell(51.52, 51.50, -0.11, -0.14)
        quarters = cell.quarters()
        assert len(quarters) == 4
        assert sum(q.side_m for q in quarters) == pytest.approx(2 * cell.side_m, rel=0.01)

    def test_the_half_diagonal_exceeds_the_side(self) -> None:
        cell = Cell(51.52, 51.50, -0.11, -0.14)
        assert cell.half_diagonal_m > cell.side_m / 2


class TestCertifiedRadius:
    def test_it_is_the_conservative_fraction_of_reach(self) -> None:
        assert certified_radius(1000.0) == pytest.approx(300.0)
        assert certified_radius(0.0) == 0.0


class TestSweepRecovers:
    async def collect(self, endpoint: Endpoint) -> set[int]:
        found: set[int] = set()
        async for result in sweep(endpoint.probe, BOUNDS, cap=CAP):
            found.update(b.id for b in result.bubbles)
        return found

    async def test_a_faithful_endpoint_yields_every_point(self) -> None:
        # A lattice fine enough that the box saturates the cap, so the sweep must
        # subdivide, and every point must still come out.
        field = grid_field(40, 0.0005, 51.505, -0.135)
        endpoint = Endpoint(field)
        found = await self.collect(endpoint)
        assert found == {b.id for b in field}

    async def test_a_sparse_field_is_recovered_and_terminates(self) -> None:
        # Even a sparse field subdivides under the 0.30 shrink — the certified
        # disc is only a third of the reach — but it must recover every point and
        # stop, far short of the empty-box brute-force count.
        field = grid_field(3, 0.006, 51.503, -0.137)
        endpoint = Endpoint(field)
        found = await self.collect(endpoint)
        assert found == {b.id for b in field}
        assert endpoint.calls < 200

    async def test_an_empty_region_costs_one_probe(self) -> None:
        endpoint = Endpoint([])
        found = await self.collect(endpoint)
        assert found == set()
        assert endpoint.calls == 1

    async def test_rim_loss_is_survived_by_the_certified_shrink(self) -> None:
        # The endpoint drops points beyond CERTIFY_FRACTION of its radius. Because
        # the sweep only trusts the inner disc and subdivides to tile it, recall
        # is still complete — this is the whole point of the shrink.
        field = grid_field(40, 0.0005, 51.505, -0.135)
        endpoint = Endpoint(field, loss_beyond=CERTIFY_FRACTION, drop=0.5)
        found = await self.collect(endpoint)
        assert found == {b.id for b in field}


class TestAudit:
    async def sweep_results(self, endpoint: Endpoint) -> list[Result]:
        return [r async for r in sweep(endpoint.probe, BOUNDS, cap=CAP)]

    async def test_a_faithful_endpoint_shows_no_violations(self) -> None:
        field = grid_field(40, 0.0005, 51.505, -0.135)
        endpoint = Endpoint(field)
        results = await self.sweep_results(endpoint)
        violations, pairs = audit(results, field)
        assert pairs > 0
        assert violations == 0

    async def test_loss_inside_the_certified_radius_is_caught(self) -> None:
        # An endpoint that drops points even inside the certified disc — worse
        # than the real one — must trip the audit, or the audit is worthless.
        field = grid_field(40, 0.0005, 51.505, -0.135)
        endpoint = Endpoint(field, loss_beyond=0.1, drop=0.5)
        results = await self.sweep_results(endpoint)
        violations, pairs = audit(results, field)
        assert violations > 0

    def test_audit_is_empty_when_there_is_nothing_to_check(self) -> None:
        assert audit([], []) == (0, 0)


class TestFloor:
    async def test_an_impossible_density_terminates_at_the_floor(self) -> None:
        # More than the cap of points at essentially one spot: the sweep cannot
        # separate them, and must stop at the floor rather than recurse for ever.
        field = [
            Bubble(id=i, lat=51.510 + i * 1e-7, lon=-0.125, captured_at=None, prev=None, next=None)
            for i in range(CAP + 50)
        ]
        endpoint = Endpoint(field)
        saturated_at_floor = 0
        async for result in sweep(endpoint.probe, BOUNDS, cap=CAP, floor_m=50.0):
            if result.saturated:
                saturated_at_floor += 1
        assert saturated_at_floor > 0  # it terminated, and flagged the stack
