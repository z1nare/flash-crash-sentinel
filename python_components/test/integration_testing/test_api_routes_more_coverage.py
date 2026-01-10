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

    # Make the environment deterministic:
    # - On some machines the IB client dependencies are installed, so controller.ib_service exists.
    # - In CI they may not, so controller.ib_service is None.
    # We explicitly disable IB service here to test the "optional dependency absent" contract.
    import api.routes as routes_mod
    if getattr(routes_mod, "_controller_instance", None) is not None:
        try:
            routes_mod._controller_instance.ib_service = None
        except Exception:
            pass

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


def test_api_news_rejects_missing_headline(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "NEWS",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "headline": "   ",
        "url": "https://example.com",
    }
    r = client.post("/api/news", json=payload)
    assert r.status_code == 400
    assert "Headline is required" in r.json()["detail"]


def test_api_news_rejects_missing_url(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "NEWS",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "headline": "Breaking news",
        "url": "   ",
    }
    r = client.post("/api/news", json=payload)
    assert r.status_code == 400
    assert "URL is required" in r.json()["detail"]


def test_api_news_happy_path_persists_article_csv(tmp_path, monkeypatch):
    """
    Happy path for POST /api/news:
    - returns 200 and sentiment fields
    - persists a row into dataInCsv/articles.csv
    """
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "NEWS",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "headline": "Market rallies on strong earnings",
        "url": "https://example.com/article",
    }

    r = client.post("/api/news", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["ticker"] == "TEST"
    assert isinstance(body["sentiment_score"], (int, float))
    assert body["sentiment_label"] in {"positive", "negative", "neutral"}

    # Persistence evidence (controller writes to articles.csv)
    csv_path = tmp_path / "dataInCsv" / "articles.csv"
    assert csv_path.exists()
    content = csv_path.read_text(encoding="utf-8")
    assert "TEST" in content
    assert "Market rallies" in content


def test_api_sentiment_analyze_rejects_empty_text(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post("/api/sentiment/analyze", json={"text": "   "})
    assert r.status_code == 400
    assert "Text is required" in r.json()["detail"]


def test_api_sentiment_analyze_happy_path_returns_label_and_score(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post("/api/sentiment/analyze", json={"text": "Stocks rallied strongly after the earnings beat expectations."})
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert isinstance(body["sentiment_score"], (int, float))
    assert body["sentiment_label"] in {"positive", "negative", "neutral"}
    assert "text" in body


def test_api_tick_rejects_negative_volume(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "TICK",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": -1,
    }
    r = client.post("/api/tick", json=payload)
    assert r.status_code == 400
    assert "non-negative" in r.json()["detail"]


def test_api_plots_view_serves_allowed_file_when_present(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    # Create an allowed plot filename
    (tmp_path / "plots" / "1_sentinel_dashboard.html").write_text("<html>ok</html>", encoding="utf-8")

    r = client.get("/api/plots/view/1_sentinel_dashboard.html")
    assert r.status_code == 200
    assert "text/html" in (r.headers.get("content-type") or "")


def test_api_status_reports_latest_metrics_from_seeded_csv(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    # Seed a ticker CSV with a mix of invalid/zero rows and one valid metrics row.
    csv_path = tmp_path / "historicalData" / "AAA.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,ticker,VPIN,vol",
                "not-a-date,AAA,0.5,0.02",         # invalid timestamp -> dropped
                "2026-01-01T09:31:00,AAA,0,0",      # both zero -> filtered out
                "2026-01-01T09:32:00,AAA,0.7,0.03", # valid
            ]
        ),
        encoding="utf-8",
    )

    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["services_ready"] is True
    assert "AAA" in body["latest_metrics"]
    assert body["latest_metrics"]["AAA"]["vpin"] == 0.7
    assert body["latest_metrics"]["AAA"]["volatility"] == 0.03


def test_api_ib_connect_returns_available_false_when_service_absent(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    import api.routes as routes_mod
    if getattr(routes_mod, "_controller_instance", None) is not None:
        routes_mod._controller_instance.ib_service = None

    r = client.post("/api/ib/connect")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is False
    assert body["available"] is False


def test_api_ib_stream_start_returns_error_when_service_absent(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    import api.routes as routes_mod
    if getattr(routes_mod, "_controller_instance", None) is not None:
        routes_mod._controller_instance.ib_service = None

    r = client.post("/api/ib/stream/start?ticker=SPY")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is False
    assert "error" in body


def test_api_ib_stream_stop_returns_error_when_service_absent(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    import api.routes as routes_mod
    if getattr(routes_mod, "_controller_instance", None) is not None:
        routes_mod._controller_instance.ib_service = None

    r = client.post("/api/ib/stream/stop")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is False or body.get("available") is False


@pytest.mark.xfail(
    reason=(
        "Known mismatch: POST /api/news persists to dataInCsv/articles.csv, "
        "but plotting expects dataInCsv/articles_with_sentiment.csv. "
        "This prevents news submitted via API from appearing in sentiment-impact plots."
    ),
    strict=False,
)
def test_imperfection_news_should_persist_articles_with_sentiment_csv(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    payload = {
        "event_type": "NEWS",
        "timestamp": datetime(2026, 1, 1, 9, 30).isoformat(),
        "ticker": "TEST",
        "headline": "News that should be visible in plots",
        "url": "https://example.com/article",
    }

    r = client.post("/api/news", json=payload)
    assert r.status_code == 200

    expected = tmp_path / "dataInCsv" / "articles_with_sentiment.csv"
    assert expected.exists()


@pytest.mark.xfail(
    reason=(
        "Future requirement: /api/metrics/history should support an aggregate mode (no ticker) "
        "to return latest metrics across tickers for dashboards. Current implementation requires ticker."
    ),
    strict=False,
)
def test_imperfection_metrics_history_should_support_aggregate_mode(tmp_path, monkeypatch):
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    # Expected behaviour (ideal): 200 with aggregated metrics.
    r = client.get("/api/metrics/history")
    assert r.status_code == 200


