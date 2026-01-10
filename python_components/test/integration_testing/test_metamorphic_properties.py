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

    # Ensure plot service has a sentiment file path (can be empty)
    (tmp_path / "dataInCsv" / "articles_with_sentiment.csv").write_text(
        "timestamp,ticker,headline,sentiment_score,sentiment_label,url\n", encoding="utf-8"
    )

    import api.main as main_mod
    import api.routes as routes_mod
    import services.plotService as plot_mod

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


def test_metamorphic_plot_generation_stable_under_row_permutation(tmp_path, monkeypatch):
    """
    Oracle-free property: row permutation (then read+sort by timestamp inside plotting)
    should not change the count of plots generated, and should not crash.
    """
    ts0 = datetime(2026, 1, 1, 9, 30)
    rows = []
    for i in range(250):
        ts = ts0 + timedelta(minutes=i)
        rows.append(
            {
                "event_type": "TICK",
                "timestamp": ts.isoformat(),
                "ticker": "META",
                "open": 100 + i * 0.01,
                "high": 100 + i * 0.01 + 0.1,
                "low": 100 + i * 0.01 - 0.1,
                "close": 100 + i * 0.01 + 0.02,
                "volume": 1000,
                "VPIN": 0.5,
                "vol": 0.01,
            }
        )

    df = pd.DataFrame(rows)
    df_perm = df.sample(frac=1.0, random_state=123).reset_index(drop=True)

    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    df_perm.to_csv(tmp_path / "historicalData" / "META.csv", index=False)

    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post("/api/plots/generate", json={"ticker": "META"})
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert len(body["plots_generated"]) == 5


def test_metamorphic_metrics_history_ignores_rows_with_missing_metrics(tmp_path, monkeypatch):
    """
    Oracle-free property: adding rows with missing VPIN/vol should not increase the
    number of returned metric samples because endpoint filters empty metrics.
    """
    csv_path = tmp_path / "historicalData" / "MM.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    csv_path.write_text(
        "\n".join(
            [
                "timestamp,ticker,VPIN,vol",
                "2026-01-01T09:30:00,MM,0.2,0.01",
                "2026-01-01T09:31:00,MM,,",          # missing both
                "2026-01-01T09:32:00,MM,0.3,0.02",
                "2026-01-01T09:33:00,MM,,",          # missing both
            ]
        ),
        encoding="utf-8",
    )

    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/api/metrics/history?ticker=MM&limit=100")
    assert r.status_code == 200
    rows = r.json()
    # Should only contain the 2 rows with actual metrics
    assert len(rows) == 2


def test_metamorphic_sentiment_analyze_invariant_under_whitespace(tmp_path, monkeypatch):
    """
    Oracle-free property: adding surrounding whitespace should not change the
    sentiment label domain, and should never crash.
    """
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    text = "Stocks rallied strongly after earnings."
    r1 = client.post("/api/sentiment/analyze", json={"text": text})
    r2 = client.post("/api/sentiment/analyze", json={"text": f"   {text}   "})

    assert r1.status_code == 200
    assert r2.status_code == 200
    b1 = r1.json()
    b2 = r2.json()
    assert b1["sentiment_label"] in {"positive", "negative", "neutral"}
    assert b2["sentiment_label"] in {"positive", "negative", "neutral"}


