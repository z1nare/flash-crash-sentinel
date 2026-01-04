import importlib
from datetime import datetime

import pytest
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch):
    """
    Create an isolated FastAPI app instance that writes into tmp_path instead of the real repo folders.
    """
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")

    # Ensure expected folders exist
    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)

    # Reload modules so they pick up env vars + rebuild controller singleton
    import api.main as main_mod
    import api.routes as routes_mod

    importlib.reload(routes_mod)
    importlib.reload(main_mod)

    return main_mod.app


def test_api_tick_happy_path_persists_rows(tmp_path, monkeypatch):
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
        "volume": 100000,
    }

    r = client.post("/api/tick", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True

    # CSV created
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    assert csv_path.exists()
    content = csv_path.read_text(encoding="utf-8")
    assert "TEST" in content

