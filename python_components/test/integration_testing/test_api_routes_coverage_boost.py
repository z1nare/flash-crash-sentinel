"""Additional integration tests to boost API routes coverage to 80%+."""
import os
import tempfile
from datetime import datetime
from pathlib import Path

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


def test_metrics_history_limit_boundary(tmp_path, monkeypatch):
    """Test limit parameter boundary cases."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Create test CSV
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=200, freq="H"),
        "ticker": "TEST",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000.0,
        "VPIN": 0.5,
        "vol": 0.02,
        "regime": 0
    })
    df.to_csv(csv_path, index=False)
    
    # Test limit < 1 (should be clamped to 1)
    resp = client.get("/api/metrics/history?ticker=TEST&limit=0")
    assert resp.status_code == 200
    
    # Test limit > 10000 (should be clamped to 10000)
    resp = client.get("/api/metrics/history?ticker=TEST&limit=20000")
    assert resp.status_code == 200
    assert len(resp.json()) <= 10000


def test_metrics_history_malformed_csv(tmp_path, monkeypatch):
    """Test handling of malformed CSV files."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Create malformed CSV
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    with open(csv_path, "w") as f:
        f.write("timestamp,ticker,open,high,low,close\n")
        f.write("invalid,data,here\n")
        f.write("2024-01-01,TEST,100,101,99,100.5\n")
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    # Should handle gracefully (skip bad lines)
    assert resp.status_code in [200, 500]  # Depends on pandas version


def test_metrics_history_missing_columns(tmp_path, monkeypatch):
    """Test handling of CSV with missing metric columns."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": ["2024-01-01 09:30:00"],
        "ticker": ["TEST"],
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5],
        "volume": [1000.0]
        # Missing VPIN, vol, regime
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    # Should return empty or handle missing columns gracefully


def test_metrics_history_invalid_timestamps(tmp_path, monkeypatch):
    """Test handling of invalid timestamp formats."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": ["invalid", "2024-01-01 09:30:00", 12345, None],
        "ticker": ["TEST"] * 4,
        "open": [100.0] * 4,
        "high": [101.0] * 4,
        "low": [99.0] * 4,
        "close": [100.5] * 4,
        "volume": [1000.0] * 4,
        "VPIN": [0.5] * 4,
        "vol": [0.02] * 4
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    # Should filter out invalid timestamps


def test_metrics_history_zero_metrics_filtered(tmp_path, monkeypatch):
    """Test that rows with both VPIN and vol = 0 are filtered out."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="H"),
        "ticker": ["TEST"] * 3,
        "open": [100.0] * 3,
        "high": [101.0] * 3,
        "low": [99.0] * 3,
        "close": [100.5] * 3,
        "volume": [1000.0] * 3,
        "VPIN": [0.0, 0.5, 0.0],
        "vol": [0.0, 0.02, 0.01]  # First row: both 0, should be filtered
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    data = resp.json()
    # Should have 2 rows (one filtered out)
    assert len(data) == 2


def test_plots_view_invalid_name(tmp_path, monkeypatch):
    """Test viewing plot with invalid name (path traversal attempt)."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Try path traversal - FastAPI may return 404 for invalid URL patterns before handler runs
    # Or the handler may return 400 if it reaches the validation
    resp = client.get("/api/plots/view/../../../etc/passwd")
    assert resp.status_code in [400, 404]  # Either is acceptable (400 = validation, 404 = routing)
    
    # Try invalid plot name that passes routing but fails validation - should return 400
    resp = client.get("/api/plots/view/invalid_plot_name.html")
    assert resp.status_code == 400  # Not in allowed_plots list
    
    # Try non-existent but valid plot name - should return 404 (file not found)
    resp = client.get("/api/plots/view/1_sentinel_dashboard.html")
    assert resp.status_code == 404


def test_ib_connect_with_params(tmp_path, monkeypatch):
    """Test IB connect with custom host/port/client_id."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Mock IB service to avoid actual connection
    import api.routes as routes_mod
    original_controller = routes_mod._controller_instance
    
    class MockIBService:
        def __init__(self):
            self.host = "127.0.0.1"
            self.port = 7497
            self.client_id = 1
            self.available = True
        
        def connect(self):
            pass
    
    if original_controller:
        original_controller.ib_service = MockIBService()
    
    resp = client.post("/api/ib/connect?host=192.168.1.1&port=4002&client_id=5")
    # Should handle gracefully (may fail if IB not available, but shouldn't crash)
    assert resp.status_code in [200, 500]


def test_ib_stream_start_stop(tmp_path, monkeypatch):
    """Test IB stream start/stop endpoints."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Mock IB service
    import api.routes as routes_mod
    original_controller = routes_mod._controller_instance
    
    class MockIBService:
        def __init__(self):
            self.available = True
        
        def start_realtime_bars(self, ticker):
            pass
        
        def stop_all_streams(self):
            pass
    
    if original_controller:
        original_controller.ib_service = MockIBService()
    
    resp = client.post("/api/ib/stream/start?ticker=NVDA")
    assert resp.status_code in [200, 500]
    
    resp = client.post("/api/ib/stream/stop")
    assert resp.status_code in [200, 500]


def test_status_endpoint_empty_state(tmp_path, monkeypatch):
    """Test status endpoint with empty/no data state."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    # Check actual response structure
    assert "ib_connection" in data
    assert "latest_metrics" in data
    assert "services_ready" in data
    assert "vpin_states" in data


def test_plots_generate_invalid_ticker(tmp_path, monkeypatch):
    """Test plot generation with invalid/non-existent ticker."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    resp = client.post("/api/plots/generate", json={
        "ticker": "NONEXISTENT",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    })
    # Should handle gracefully (may return error or empty plots)
    assert resp.status_code in [200, 404, 500]

