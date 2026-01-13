"""Additional unit tests to boost ServiceController coverage."""
import os
import tempfile
from pathlib import Path

import pytest

from controllers.ServiceController import ServiceController


def test_service_controller_initialization():
    """Test ServiceController initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        assert controller is not None
        assert controller.HISTORICAL_DATA_DIR == os.path.join(tmpdir, "historicalData")


def test_get_ticker_csv_path():
    """Test getting ticker CSV path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        path = controller.get_ticker_csv_path("NVDA")
        assert "NVDA.csv" in path
        assert path.endswith(".csv")
        
        # Should uppercase ticker
        path2 = controller.get_ticker_csv_path("nvda")
        assert path == path2


def test_ib_available():
    """Test IB availability check."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # IB service might be initialized but not connected
        # Check that the method works (returns bool)
        result = controller.ib_available()
        assert isinstance(result, bool)


def test_get_ib_status():
    """Test getting IB status."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        status = controller.get_ib_status()
        assert isinstance(status, dict)
        assert "available" in status
        assert "connected" in status
        assert "streaming" in status
        assert "tickers" in status


def test_disconnect_ib_no_service():
    """Test disconnecting IB when service not available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Should not raise error
        controller.disconnect_ib()


def test_stop_ib_streams_no_service():
    """Test stopping IB streams when service not available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Should not raise error
        controller.stop_ib_streams()


def test_get_volatility_service():
    """Test getting volatility service."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        vol_service = controller.get_volatility_service("NVDA")
        assert vol_service is not None


def test_get_regime_service():
    """Test getting regime service."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # get_regime_service may return None if no model is loaded (expected behavior)
        regime_service = controller.get_regime_service("NVDA")
        # Service might be None if no model available - that's expected
        # Just verify the method doesn't crash
        assert regime_service is None or hasattr(regime_service, 'predict_regime')


def test_get_sentiment_service():
    """Test getting sentiment service."""
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
        controller = ServiceController(base_dir=tmpdir)
        
        sentiment_service = controller.get_sentiment_service()
        assert sentiment_service is not None

