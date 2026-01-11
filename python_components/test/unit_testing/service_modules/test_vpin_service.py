from datetime import datetime
from types import SimpleNamespace
from services.vpin_service import VpinService


def _tick(
    ticker: str = "TEST",
    *,
    open_: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
    volume: int = 100_000,
) -> SimpleNamespace:
    return SimpleNamespace(
        event_type="TICK",
        timestamp=datetime(2026, 1, 1, 9, 30),
        ticker=ticker,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def test_process_tick_returns_none_for_non_positive_volume():
    svc = VpinService()
    assert svc.process_tick(_tick(volume=0)) is None
    assert svc.process_tick(_tick(volume=-5)) is None


def test_process_tick_exact_bucket_volume_closes_bucket_and_returns_value():
    svc = VpinService()
    out = svc.process_tick(_tick(volume=int(svc.BUCKET_VOLUME)))
    assert out is not None
    assert 0.0 <= out <= 1.0
    assert "TEST" in svc.state
    assert len(svc.state["TEST"].vpin_ratio_history) == 1


def test_process_tick_flat_candle_results_in_zero_vpin_for_filled_bucket():
    svc = VpinService()
    out = svc.process_tick(_tick(open_=100.0, close=100.0, volume=int(svc.BUCKET_VOLUME)))
    assert out == 0.0
    assert svc.state["TEST"].last_vpin == 0.0


def test_process_tick_volume_spanning_multiple_buckets_commits_multiple_entries():
    svc = VpinService()
    out = svc.process_tick(_tick(volume=int(svc.BUCKET_VOLUME * 2.5)))
    assert out is not None
    # Two full buckets committed, remainder stays in current bucket
    assert len(svc.state["TEST"].vpin_ratio_history) == 2
    assert 0.0 <= out <= 1.0


def test_process_tick_sliding_window_caps_history_at_bucket_window():
    svc = VpinService()
    # Make window small so test is fast
    svc.BUCKET_WINDOW = 3
    vol = int(svc.BUCKET_VOLUME)

    for i in range(5):  # fill 5 buckets
        out = svc.process_tick(_tick(ticker="TEST", open_=100.0, close=100.0 + i, volume=vol))
        assert out is not None

    st = svc.state["TEST"]
    assert len(st.vpin_ratio_history) == 3
    assert len(st.imbalance_history) == 3
    assert len(st.volume_history) == 3


def test_process_tick_clamps_vpin_into_unit_interval_for_extreme_returns():
    svc = VpinService()
    # Big up candle -> buy_ratio tends to 1.0, imbalance magnitude tends to volume
    out_up = svc.process_tick(_tick(ticker="UP", open_=1.0, close=10.0, volume=int(svc.BUCKET_VOLUME)))
    assert out_up is not None
    assert 0.0 <= out_up <= 1.0

    # Big down candle -> buy_ratio tends to 0.0, imbalance magnitude tends to volume
    out_dn = svc.process_tick(_tick(ticker="DN", open_=10.0, close=1.0, volume=int(svc.BUCKET_VOLUME)))
    assert out_dn is not None
    assert 0.0 <= out_dn <= 1.0


