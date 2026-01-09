import importlib
from datetime import datetime

import pytest
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch, *, make_hist_dir: bool = True, make_plots_dir: bool = True):
    """
    Create an isolated FastAPI app instance that writes into tmp_path instead of the real repo folders.
    """
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    # Ensure the optional smoke test is not accidentally activated by caller shell env
    monkeypatch.delenv("RUN_FINBERT_TESTS", raising=False)

    if make_hist_dir:
        (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)
    if make_plots_dir:
        (tmp_path / "plots").mkdir(parents=True, exist_ok=True)

    import api.main as main_mod
    import api.routes as routes_mod
    import services.plotService as plot_mod

    # Force plots to land in tmp_path/plots for endpoints that use DEFAULT_OUTPUT_DIR.
    plot_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    routes_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")

    importlib.reload(routes_mod)
    importlib.reload(main_mod)

    # Re-apply after reload
    import services.plotService as plot_mod2
    import api.routes as routes_mod2

    plot_mod2.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    routes_mod2.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")

    return main_mod.app


def test_api_tick_rejects_high_less_than_low(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "TICK",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "open": 100.0,
        "high": 99.0,
        "low": 101.0,
        "close": 100.5,
        "volume": 1,
    }

    r = client.post("/api/tick", json=payload)
    assert r.status_code == 400
    assert "High must be" in r.json()["detail"]


def test_api_tick_rejects_open_outside_range(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "TICK",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "open": 200.0,  # outside [low, high]
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1,
    }

    r = client.post("/api/tick", json=payload)
    assert r.status_code == 400
    assert "Open must be between" in r.json()["detail"]


def test_api_metrics_history_requires_ticker_param(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/metrics/history")
    assert r.status_code == 400
    assert "Ticker parameter is required" in r.json()["detail"]


def test_api_metrics_history_404_when_ticker_file_missing(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/metrics/history?ticker=MISSING")
    assert r.status_code == 404
    assert "No data found for ticker" in r.json()["detail"]


def test_api_list_tickers_returns_empty_when_hist_dir_missing(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch, make_hist_dir=False)
    client = TestClient(app)

    r = client.get("/api/tickers")
    assert r.status_code == 200
    body = r.json()
    assert body["tickers"] == []
    # Implementation detail: ServiceController may create the historicalData dir at init time
    # (depending on version/env). Accept both shapes while asserting "empty inventory".
    if "message" in body:
        assert "directory not found" in body["message"]
    else:
        assert body.get("count", 0) == 0


def test_api_status_empty_without_csvs(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["services_ready"] is True
    assert body["latest_metrics"] == {}
    assert "vpin_states" in body


def test_api_ib_disconnect_returns_available_false_when_service_absent(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post("/api/ib/disconnect")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["available"] is False
    assert body["connected"] is False


def test_api_plots_list_returns_message_when_plots_dir_missing(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch, make_plots_dir=False)
    client = TestClient(app)

    r = client.get("/api/plots/list")
    assert r.status_code == 200
    body = r.json()
    assert body["plots"] == []
    assert "Generate plots first" in body["message"]


def test_api_plots_view_rejects_invalid_plot_name(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/plots/view/not_allowed.html")
    assert r.status_code == 400
    assert "Invalid plot name" in r.json()["detail"]


def test_api_plots_view_404_when_allowed_plot_missing(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/plots/view/1_sentinel_dashboard.html")
    assert r.status_code == 404
    assert "Generate plots first" in r.json()["detail"]


@pytest.mark.xfail(
    reason=(
        "Known mismatch: plot generator writes ticker-prefixed filenames (e.g., TEST_1_...) "
        "but /api/plots/view only allows non-prefixed names (1_...). "
        "This makes returned URLs from /api/plots/generate non-browseable for ticker-prefixed outputs."
    ),
    strict=False,
)
def test_imperfection_plot_view_should_allow_ticker_prefixed_files(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    # Simulate a ticker-prefixed plot file created by generate_all_plots()
    (tmp_path / "plots" / "TEST_1_sentinel_dashboard.html").write_text("<html>ok</html>", encoding="utf-8")

    # Expected behaviour (ideal): should serve the plot. Current behaviour: 400 (invalid name).
    r = client.get("/api/plots/view/TEST_1_sentinel_dashboard.html")
    assert r.status_code == 200


@pytest.mark.xfail(
    reason=(
        "Known mismatch: /api/plots/list only looks for non-ticker-prefixed filenames "
        "(1_sentinel_dashboard.html, etc). When plots are generated per ticker "
        "(TEST_1_sentinel_dashboard.html, etc), list endpoint reports 0 available."
    ),
    strict=False,
)
def test_imperfection_plots_list_should_include_ticker_prefixed_outputs(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    # Simulate per-ticker plot outputs
    for name in (
        "PERF_1_sentinel_dashboard.html",
        "PERF_2_liquidity_heatmap.html",
        "PERF_3_volatility_cone.html",
        "PERF_4_sentiment_impact.html",
        "PERF_5_crash_gauge.html",
    ):
        (tmp_path / "plots" / name).write_text("<html>ok</html>", encoding="utf-8")

    r = client.get("/api/plots/list")
    assert r.status_code == 200
    body = r.json()

    # Expected behaviour (ideal): should detect 5 ticker outputs.
    assert body["total_available"] == 5


