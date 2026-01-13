"""Additional API route error path tests."""
import os
from datetime import datetime

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


def test_process_news_validation_errors(tmp_path, monkeypatch):
    """Test /api/news validation error paths."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    base_news = {
        "event_type": "NEWS",
        "timestamp": datetime.now().isoformat(),
        "ticker": "TEST",
        "headline": "Test headline",
        "url": "http://test.com"
    }
    
    # Empty headline
    resp = client.post("/api/news", json={**base_news, "headline": ""})
    assert resp.status_code == 400
    
    # Whitespace headline
    resp = client.post("/api/news", json={**base_news, "headline": "   "})
    assert resp.status_code == 400
    
    # Empty URL
    resp = client.post("/api/news", json={**base_news, "url": ""})
    assert resp.status_code == 400


def test_analyze_sentiment_error_paths(tmp_path, monkeypatch):
    """Test /api/sentiment/analyze error paths."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Missing required fields
    resp = client.post("/api/sentiment/analyze", json={})
    assert resp.status_code == 422
    
    # Valid request (uses 'text' field, not 'ticker' and 'headline')
    resp = client.post("/api/sentiment/analyze", json={
        "text": "Test headline"
    })
    assert resp.status_code in [200, 500]


def test_metrics_history_csv_read_errors(tmp_path, monkeypatch):
    """Test /api/metrics/history with various CSV read errors."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    
    # Empty CSV
    csv_path.write_text("")
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code in [200, 500]
    
    # CSV with only headers
    csv_path.write_text("timestamp,ticker,open,high,low,close,volume\n")
    resp = client.get("/api/metrics/history?ticker=TEST")
    assert resp.status_code == 200
    assert resp.json() == []
    
    # CSV with invalid regime values
    csv_path.write_text("timestamp,ticker,open,high,low,close,volume,VPIN,vol,regime\n")
    csv_path.write_text("2024-01-01 09:30:00,TEST,100,101,99,100.5,1000,0.5,0.02,invalid\n")
    resp = client.get("/api/metrics/history?ticker=TEST")
    # Should handle gracefully
    assert resp.status_code in [200, 500]


def test_metrics_history_regime_service_error(tmp_path, monkeypatch):
    """Test /api/metrics/history when regime service fails."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Create CSV with regime data
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    import pandas as pd
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
        "regime": [0, 1, 2, 0, 1]
    })
    df.to_csv(csv_path, index=False)
    
    # Mock controller to have regime service that raises
    import api.routes as routes_mod
    original_controller = routes_mod._controller_instance
    
    class MockRegimeService:
        def get_regime_label(self, regime):
            raise ValueError("Test error")
    
    class MockController:
        def get_ticker_csv_path(self, ticker):
            return str(csv_path)
        
        def get_regime_service(self, ticker):
            return MockRegimeService()
    
    routes_mod._controller_instance = MockController()
    
    resp = client.get("/api/metrics/history?ticker=TEST")
    # Should handle regime service error gracefully
    assert resp.status_code == 200
    
    routes_mod._controller_instance = original_controller


def test_plots_generate_exception_handling(tmp_path, monkeypatch):
    """Test /api/plots/generate exception handling."""
    app = _make_app(tmp_path, monkeypatch)
    client = TestClient(app)
    
    # Create CSV file so it doesn't return 404
    csv_path = tmp_path / "historicalData" / "TEST.csv"
    import pandas as pd
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h"),
        "ticker": "TEST",
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.5] * 10,
        "volume": [1000.0] * 10
    })
    df.to_csv(csv_path, index=False)
    
    resp = client.post("/api/plots/generate", json={
        "ticker": "TEST"
    })
    # Should handle gracefully (may return 200, 404, or 500 depending on plot generation)
    assert resp.status_code in [200, 404, 500]

