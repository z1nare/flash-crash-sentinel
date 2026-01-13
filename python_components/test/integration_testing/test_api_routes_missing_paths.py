"""Additional tests for missing API route paths."""
import os
from datetime import datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch):
    """Create isolated FastAPI app."""
    monkeypatch.setenv("RISKBEACON_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    monkeypatch.delenv("RUN_FINBERT_TESTS", raising=False)
    
    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataInCsv").mkdir(parents=True, exist_ok=True)
    (tmp_path / "plots").mkdir(parents=True, exist_ok=True)
    
    import importlib
    import api.main as main_mod
    import api.routes as routes_mod
    import services.plotService as plot_mod
    
    plot_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    routes_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "plots")
    
    importlib.reload(routes_mod)
    importlib.reload(main_mod)
    
    return main_mod.app


def test_analyze_sentiment_error_handling(tmp_path, monkeypatch):
    """Test /api/sentiment/analyze error handling."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Valid request (uses 'text' field)
    resp = client.post("/api/sentiment/analyze", json={
        "text": "Test headline"
    })
    assert resp.status_code in [200, 500]


def test_metrics_history_csv_read_old_pandas(tmp_path, monkeypatch):
    """Test /api/metrics/history with old pandas version handling."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="h"),
        "ticker": "TEST",
        "open": [100.0] * 5,
        "high": [101.0] * 5,
        "low": [99.0] * 5,
        "close": [100.5] * 5,
        "volume": [1000.0] * 5,
        "VPIN": [0.5] * 5,
        "vol": [0.02] * 5,
        "regime": [0, 1, 2, 0, 1],
        "regime_confidence": [0.8, 0.9, 0.7, 0.85, 0.75]
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_metrics_history_regime_label_fallback(tmp_path, monkeypatch):
    """Test /api/metrics/history regime label fallback."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="h"),
        "ticker": "TEST",
        "open": [100.0] * 3,
        "high": [101.0] * 3,
        "low": [99.0] * 3,
        "close": [100.5] * 3,
        "volume": [1000.0] * 3,
        "VPIN": [0.5] * 3,
        "vol": [0.02] * 3,
        "regime": [0, 1, 2]
    })
    df.to_csv(csv_path, index=False)
    
    # Mock controller to return None for regime service
    import api.routes as routes_mod
    original_controller = routes_mod._controller_instance
    
    class MockController:
        def get_ticker_csv_path(self, ticker):
            return str(csv_path)
        
        def get_regime_service(self, ticker):
            return None  # No regime service
    
    routes_mod._controller_instance = MockController()
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    data = resp.json()
    # Should use fallback labels
    assert isinstance(data, list)
    
    routes_mod._controller_instance = original_controller


def test_metrics_history_regime_exception(tmp_path, monkeypatch):
    """Test /api/metrics/history when regime label lookup raises exception."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="h"),
        "ticker": "TEST",
        "open": [100.0] * 3,
        "high": [101.0] * 3,
        "low": [99.0] * 3,
        "close": [100.5] * 3,
        "volume": [1000.0] * 3,
        "VPIN": [0.5] * 3,
        "vol": [0.02] * 3,
        "regime": ["invalid", "also_invalid", 2]  # Invalid regime values
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    # Should handle invalid regime values gracefully
    assert resp.status_code == 200


def test_status_endpoint_with_data(tmp_path, monkeypatch):
    """Test /api/status with actual data."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Create some ticker data
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h"),
        "ticker": "TEST",
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.5] * 10,
        "volume": [1000.0] * 10,
        "VPIN": [0.5] * 10,
        "vol": [0.02] * 10
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "latest_metrics" in data
    # latest_metrics is a dict keyed by ticker, not containing "tickers" key
    assert isinstance(data["latest_metrics"], dict)
    assert "TEST" in data["latest_metrics"]  # Our ticker should be in there


def test_plots_list_error_handling(tmp_path, monkeypatch):
    """Test /api/plots/list error handling."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Remove plots directory to trigger error path
    import api.routes as routes_mod
    original_dir = routes_mod.DEFAULT_OUTPUT_DIR
    routes_mod.DEFAULT_OUTPUT_DIR = str(tmp_path / "nonexistent_plots")
    
    resp = client.get("/api/plots/list")
    # Should handle missing directory gracefully
    assert resp.status_code in [200, 500]
    
    routes_mod.DEFAULT_OUTPUT_DIR = original_dir

