import math
from datetime import datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from backend.models.domain import TickerDTO
from services.vpin_service import VpinService


def _ticker_dto(ticker: str, ts: datetime, o: float, h: float, l: float, c: float, v: int) -> TickerDTO:
    return TickerDTO(
        event_type="TICK",
        timestamp=ts,
        ticker=ticker,
        open=float(o),
        high=float(h),
        low=float(l),
        close=float(c),
        volume=int(v),
    )


@settings(max_examples=75, deadline=None)
@given(
    # Ensure open>0 to avoid divide-by-zero in return calc.
    open_=st.floats(min_value=1.0, max_value=10_000.0, allow_nan=False, allow_infinity=False),
    ret=st.floats(min_value=-0.2, max_value=0.2, allow_nan=False, allow_infinity=False),
    volume=st.integers(min_value=1, max_value=250_000),
)
def test_vpin_always_in_unit_interval(open_: float, ret: float, volume: int):
    """
    Property-based invariant for VPIN:
    - VPIN must be in [0,1] when emitted.

    We only have OHLC bars (not tick-by-tick trade direction), so VPIN uses a heuristic buy_ratio,
    but it still must be clamped and stable in the unit interval.
    """
    svc = VpinService()
    ts = datetime(2026, 1, 1, 9, 30, 0)

    close = open_ * (1.0 + ret)
    high = max(open_, close) * 1.001
    low = min(open_, close) * 0.999

    out = svc.process_tick(_ticker_dto("TEST", ts, open_, high, low, close, volume))
    if out is None:
        # Not all ticks complete a bucket; that's fine.
        return

    assert 0.0 <= float(out) <= 1.0
    assert math.isfinite(float(out))


def test_vpin_zero_or_negative_volume_produces_no_metric():
    svc = VpinService()
    ts = datetime(2026, 1, 1, 9, 30, 0)

    dto_zero = _ticker_dto("TEST", ts, 100, 101, 99, 100.5, 0)
    assert svc.process_tick(dto_zero) is None

    dto_neg = _ticker_dto("TEST", ts + timedelta(seconds=5), 100, 101, 99, 100.5, -10)
    assert svc.process_tick(dto_neg) is None

