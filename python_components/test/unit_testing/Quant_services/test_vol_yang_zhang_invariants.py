import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from services.vol_service import VolatilityService


def _daily_bar_series(open_: float, close_: float) -> tuple[float, float, float, float]:
    high = max(open_, close_) * 1.001
    low = min(open_, close_) * 0.999
    return float(open_), float(high), float(low), float(close_)


@settings(max_examples=50, deadline=None)
@given(
    n=st.integers(min_value=2, max_value=30),
    base=st.floats(min_value=10.0, max_value=10_000.0, allow_nan=False, allow_infinity=False),
    rets=st.lists(
        st.floats(min_value=-0.15, max_value=0.15, allow_nan=False, allow_infinity=False),
        min_size=2,
        max_size=30,
    ),
)
def test_yang_zhang_vol_is_non_negative_and_finite(n: int, base: float, rets: list[float]):
    """
    Property-based invariant:
    - Yang-Zhang volatility must be finite and >= 0 for valid OHLC daily bars.
    """
    n = min(n, len(rets))
    ts0 = datetime(2026, 1, 1)

    prices = [base]
    for r in rets[:n]:
        prices.append(prices[-1] * (1.0 + r))

    rows = []
    idx = []
    for i in range(n):
        o, c = prices[i], prices[i + 1]
        o, h, l, c = _daily_bar_series(o, c)
        rows.append({"open": o, "high": h, "low": l, "close": c})
        idx.append(ts0 + timedelta(days=i))

    daily_df = pd.DataFrame(rows, index=pd.to_datetime(idx))
    vol = VolatilityService(csv_path="")._calculate_yang_zhang(daily_df)

    assert math.isfinite(float(vol))
    assert float(vol) >= 0.0


def test_yang_zhang_returns_zero_for_insufficient_days():
    svc = VolatilityService(csv_path="")
    df = pd.DataFrame([{"open": 100, "high": 101, "low": 99, "close": 100.5}], index=[pd.Timestamp("2026-01-01")])
    assert svc._calculate_yang_zhang(df) == 0.0

