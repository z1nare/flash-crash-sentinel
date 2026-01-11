from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd

from services.vol_service import VolatilityService


def _tick(day_offset: int, *, open_: float, high: float, low: float, close: float) -> SimpleNamespace:
    ts = datetime(2026, 1, 1, 9, 30) + timedelta(days=day_offset)
    return SimpleNamespace(
        event_type="TICK",
        timestamp=ts,
        ticker="TEST",
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1000,
    )


def test_process_tick_returns_zero_for_invalid_timestamp():
    svc = VolatilityService(csv_path="unused.csv")
    # Use a minimal duck-typed object here rather than TickerDTO: Pydantic will
    # correctly reject invalid datetimes before VolatilityService sees them.
    bad = SimpleNamespace(
        event_type="TICK",
        timestamp="not-a-date",
        ticker="TEST",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1,
    )
    assert svc.process_tick(bad) == 0.0


def test_process_tick_daily_cache_rollover_requires_two_completed_days():
    svc = VolatilityService(csv_path="unused.csv")

    # Day 0 -> initializes current candle, no completed days yet
    assert svc.process_tick(_tick(0, open_=100, high=101, low=99, close=100.5)) == 0.0

    # Day 1 -> commits day 0 as completed; still only 1 completed day -> 0.0
    assert svc.process_tick(_tick(1, open_=101, high=102, low=100, close=101.5)) == 0.0

    # Day 2 -> commits day 1, now we have 2 completed days -> volatility should be finite
    vol = svc.process_tick(_tick(2, open_=102, high=103, low=101, close=102.5))
    assert isinstance(vol, float)
    assert np.isfinite(vol)
    assert vol >= 0.0


def test_process_tick_keeps_only_last_21_completed_days():
    svc = VolatilityService(csv_path="unused.csv")
    # Create 30 day rollovers
    for d in range(0, 31):
        _ = svc.process_tick(_tick(d, open_=100 + d, high=101 + d, low=99 + d, close=100.5 + d))

    assert svc._daily_df is not None
    assert len(svc._daily_df) <= 21


def test_calculate_yang_zhang_zero_for_constant_prices():
    svc = VolatilityService(csv_path="unused.csv")
    idx = pd.date_range("2026-01-01", periods=5, freq="D")
    df = pd.DataFrame(
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        index=idx,
    )
    assert svc._calculate_yang_zhang(df) == 0.0


def test_calculate_yang_zhang_positive_for_varying_prices():
    svc = VolatilityService(csv_path="unused.csv")
    idx = pd.date_range("2026-01-01", periods=5, freq="D")
    df = pd.DataFrame(
        {
            "open": [100, 101, 102, 101, 103],
            "high": [101, 102, 103, 102, 104],
            "low": [99, 100, 101, 100, 102],
            "close": [100.5, 101.2, 102.1, 101.1, 103.2],
        },
        index=idx,
    )
    vol = svc._calculate_yang_zhang(df)
    assert np.isfinite(vol)
    assert vol >= 0.0
    assert vol > 0.0


def test_calculate_rolling_volatility_returns_zeros_when_insufficient_daily_bars():
    # Only 5 days worth of minute ticks, but window=21 => zeros
    rows = []
    ts0 = datetime(2026, 1, 1, 9, 30)
    for i in range(5 * 60):
        ts = ts0 + timedelta(minutes=i)
        rows.append(
            {
                "timestamp": ts.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
            }
        )
    df = pd.DataFrame(rows)
    out = VolatilityService.calculate_rolling_volatility(df, window=21)
    assert len(out) == len(df)
    assert np.allclose(out, 0.0)


