import importlib
from datetime import datetime, timedelta

import pandas as pd
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")

    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)
    (tmp_path / "plots").mkdir(parents=True, exist_ok=True)

    # Provide a sentiment file path (can be empty for this test)
    (tmp_path / "dataInCsv" / "articles_with_sentiment.csv").write_text(
        "timestamp,ticker,headline,sentiment_score,sentiment_label,url\n", encoding="utf-8"
    )

    import api.main as main_mod
    import api.routes as routes_mod
    import services.plotService as plot_mod

    # Force plots to land in tmp_path/plots
    plot_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    routes_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")

    importlib.reload(routes_mod)
    importlib.reload(main_mod)

    # Re-apply overrides after reload
    import services.plotService as plot_mod2
    import api.routes as routes_mod2
    plot_mod2.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    routes_mod2.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")

    return main_mod.app


def test_api_plots_generate_creates_five_html_files(tmp_path, monkeypatch):
    # Seed minimal market CSV for ticker TEST
    ts0 = datetime(2026, 1, 1, 9, 30)
    rows = []
    for i in range(200):
        ts = ts0 + timedelta(minutes=i)
        rows.append(
            {
                "event_type": "TICK",
                "timestamp": ts.isoformat(),
                "ticker": "TEST",
                "open": 100 + i * 0.01,
                "high": 100 + i * 0.01 + 0.1,
                "low": 100 + i * 0.01 - 0.1,
                "close": 100 + i * 0.01 + 0.02,
                "volume": 1000,
                "VPIN": 0.5,
                "vol": 0.01,
            }
        )

    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(tmp_path / "historicalData" / "TEST.csv", index=False)

    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post("/api/plots/generate", json={"ticker": "TEST"})
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["ticker"] == "TEST"
    assert len(body["plots_generated"]) == 5

    # Assert plot files exist (ticker-prefixed format)
    plots_dir = tmp_path / "plots"
    expected = [
        "TEST_1_sentinel_dashboard.html",
        "TEST_2_liquidity_heatmap.html",
        "TEST_3_volatility_cone.html",
        "TEST_4_sentiment_impact.html",
        "TEST_5_crash_gauge.html",
    ]
    for f in expected:
        assert (plots_dir / f).exists()

