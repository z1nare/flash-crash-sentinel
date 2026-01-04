import importlib
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)
    (tmp_path / "plots").mkdir(parents=True, exist_ok=True)
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


def test_perf_plot_generation_under_target_for_3000_rows(tmp_path, monkeypatch):
    """
    Performance measurement test (RB-REQ-17 evidence).

    - Generates a synthetic 3000-row dataset and measures plot generation time.
    - By default: measure-only (CI-safe). Set PERF_STRICT=true to enforce target.
    """
    ticker = "PERF"
    ts0 = datetime(2026, 1, 1, 9, 30)

    rows = []
    px = 100.0
    for i in range(3000):
        ts = ts0 + timedelta(minutes=i)
        px = px * (1.0 + (0.0001 if i % 2 == 0 else -0.00008))
        rows.append(
            {
                "event_type": "TICK",
                "timestamp": ts.isoformat(),
                "ticker": ticker,
                "open": px,
                "high": px * 1.001,
                "low": px * 0.999,
                "close": px * 1.0002,
                "volume": 1000,
                "VPIN": 0.5,
                "vol": 0.01,
            }
        )

    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(tmp_path / "historicalData" / f"{ticker}.csv", index=False)

    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    t0 = time.perf_counter()
    r = client.post("/api/plots/generate", json={"ticker": ticker})
    t1 = time.perf_counter()

    assert r.status_code == 200
    elapsed = t1 - t0
    print(f"[PERF] /api/plots/generate elapsed={elapsed:.2f}s rows=3000")

    if os.getenv("PERF_STRICT", "false").lower().strip() == "true":
        assert elapsed < 300.0

