import importlib
import os
import time
from datetime import datetime, timedelta

import numpy as np
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)

    import api.main as main_mod
    import api.routes as routes_mod
    importlib.reload(routes_mod)
    importlib.reload(main_mod)
    return main_mod.app


def test_perf_tick_endpoint_latency_median_p95(tmp_path, monkeypatch):
    """
    Performance measurement test (RB-REQ-16 evidence).

    By default this is "measure-only" to stay CI-stable; set PERF_STRICT=true to enforce targets.
    """
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)

    base_ts = datetime(2026, 1, 1, 9, 30)

    durs_ms = []
    for i in range(75):
        payload = {
            "event_type": "TICK",
            "timestamp": (base_ts + timedelta(seconds=5 * i)).isoformat(),
            "ticker": "PERF",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            # Make VPIN bucket fill quickly so we exercise the "metrics" path.
            "volume": 100000,
        }
        t0 = time.perf_counter()
        r = client.post("/api/tick", json=payload)
        t1 = time.perf_counter()
        assert r.status_code == 200
        durs_ms.append((t1 - t0) * 1000.0)

    median = float(np.median(durs_ms))
    p95 = float(np.percentile(durs_ms, 95))

    # Print is okay in tests (shows up in CI logs as evidence)
    print(f"[PERF] /api/tick median={median:.2f}ms p95={p95:.2f}ms n={len(durs_ms)}")

    if os.getenv("PERF_STRICT", "false").lower().strip() == "true":
        assert median < 50.0
        assert p95 < 150.0

