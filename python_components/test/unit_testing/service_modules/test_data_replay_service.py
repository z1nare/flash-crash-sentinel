from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd

from services import data_replay_service as dr


def test_load_excel_data_returns_none_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "BLOOMBERG_DATA_DIR", str(tmp_path))

    svc = dr.DataReplayService(api_url="http://example.invalid")
    assert svc.load_excel_data("AAPL") is None


def test_load_excel_data_returns_none_when_required_columns_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "BLOOMBERG_DATA_DIR", str(tmp_path))
    # Create the expected file so os.path.exists(excel_file) is True
    (tmp_path / "AAPL10sec.xlsx").write_text("dummy", encoding="utf-8")

    def fake_read_excel(*_args, **_kwargs):
        # Missing 'volume'
        return pd.DataFrame({"open": [1], "high": [1], "low": [1], "close": [1]})

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)

    svc = dr.DataReplayService(api_url="http://example.invalid")
    assert svc.load_excel_data("AAPL") is None


def test_load_excel_data_normalizes_columns_converts_to_numeric_drops_invalid_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "BLOOMBERG_DATA_DIR", str(tmp_path))
    (tmp_path / "AAPL10sec.xlsx").write_text("dummy", encoding="utf-8")

    calls = {"n": 0}

    def fake_read_excel(*_args, **_kwargs):
        calls["n"] += 1
        # Use messy casing/whitespace + include one invalid row
        return pd.DataFrame(
            {
                " Open ": ["100", "BAD"],
                "HIGH": ["101", "102"],
                " low": ["99", "100"],
                "Close ": ["100.5", "101.0"],
                "VOLUME": ["10", "20"],
            }
        )

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)

    svc = dr.DataReplayService(api_url="http://example.invalid")
    df1 = svc.load_excel_data("AAPL")
    assert df1 is not None
    assert list(df1.columns) == ["open", "high", "low", "close", "volume"]
    # Invalid row should be dropped (open=BAD -> NaN)
    assert len(df1) == 1
    assert calls["n"] == 1

    # Cache hit should not call pd.read_excel again
    df2 = svc.load_excel_data("AAPL")
    assert df2 is df1
    assert calls["n"] == 1


def test_send_tick_to_api_builds_payload_and_returns_true_on_http_200(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(dr.requests, "post", fake_post)

    svc = dr.DataReplayService(api_url="http://localhost:8000", speed=1.0)
    row = pd.Series({"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 7})
    ts = datetime(2026, 1, 1, 9, 30)

    ok = svc.send_tick_to_api("AAPL", row, ts)
    assert ok is True
    assert captured["url"].endswith("/api/tick")
    assert captured["json"]["ticker"] == "AAPL"
    assert captured["json"]["timestamp"] == ts.isoformat()
    assert captured["timeout"] == 5


def test_replay_loop_updates_index_on_success_and_respects_interval(monkeypatch):
    # Arrange a service with deterministic timing and no real sleeps
    svc = dr.DataReplayService(api_url="http://example.invalid", speed=10.0)
    svc.interval_seconds = 1.0  # simplify expectations

    df = pd.DataFrame(
        [
            {"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 7},
            {"open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 8},
        ]
    )

    monkeypatch.setattr(svc, "load_excel_data", lambda _ticker: df)
    monkeypatch.setattr(dr.time, "sleep", lambda _s: None)

    base = datetime(2026, 1, 1, 9, 30)
    times = [base, base + timedelta(seconds=2)]

    class FakeDateTime:
        @staticmethod
        def now():
            # Pop so first iteration sends, then we stop
            return times.pop(0) if times else base + timedelta(seconds=999)

    monkeypatch.setattr(dr, "datetime", FakeDateTime)

    def fake_send(_ticker, _row, _ts):
        # Stop after first successful send
        svc.running = False
        return True

    monkeypatch.setattr(svc, "send_tick_to_api", fake_send)

    svc.running = True
    svc._replay_loop(["AAPL"])

    # After one send, index should advance to 1
    assert svc.current_row_index["AAPL"] == 1


def test_start_validates_tickers_and_spawns_thread_without_running_it(monkeypatch):
    svc = dr.DataReplayService(api_url="http://example.invalid", speed=1.0)

    df_ok = pd.DataFrame([{"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}])
    monkeypatch.setattr(svc, "load_excel_data", lambda t: df_ok if t.upper() == "OK" else None)

    started = {"called": False, "args": None}

    class DummyThread:
        def __init__(self, target, args, daemon):
            started["args"] = (target, args, daemon)

        def start(self):
            started["called"] = True

        def is_alive(self):
            return True

        def join(self, timeout=None):
            return None

    monkeypatch.setattr(dr.threading, "Thread", DummyThread)

    svc.start(["bad", "OK"])
    assert svc.running is True
    assert started["called"] is True


def test_start_returns_without_running_when_no_valid_tickers(monkeypatch):
    svc = dr.DataReplayService(api_url="http://example.invalid", speed=1.0)
    monkeypatch.setattr(svc, "load_excel_data", lambda _t: None)

    svc.start(["A", "B"])
    assert svc.running is False
    assert svc.thread is None


