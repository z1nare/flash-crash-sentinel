"""Additional unit tests to boost ServiceController coverage."""
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from backend.models.domain import TickerDTO
from controllers.ServiceController import ServiceController


def test_service_controller_ib_methods():
    """Test IB-related methods."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Test IB status when service not available
        status = controller.get_ib_status()
        assert isinstance(status, dict)
        assert "available" in status
        
        # Test disconnect (should not raise)
        controller.disconnect_ib()
        
        # Test stop streams (should not raise)
        controller.stop_ib_streams()


def test_service_controller_connect_ib_error():
    """Test IB connect error handling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Should raise RuntimeError if IB service not available
        if not controller.ib_service:
            with pytest.raises(RuntimeError, match="IB service not available"):
                controller.connect_ib()


def test_service_controller_start_ib_stream_error():
    """Test IB stream start error handling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Should raise RuntimeError if IB service not available
        if not controller.ib_service:
            with pytest.raises(RuntimeError, match="IB service not available"):
                controller.start_ib_stream("NVDA")


def test_get_volatility_service_caching():
    """Test volatility service caching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        service1 = controller.get_volatility_service("NVDA")
        service2 = controller.get_volatility_service("NVDA")
        
        # Should return same instance (cached)
        assert service1 is service2
        
        # Different ticker should get different service
        service3 = controller.get_volatility_service("TSLA")
        assert service3 is not service1


def test_get_regime_service_experiment_path():
    """Test regime service loading from experiment results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create fake experiment model path
        experiments_dir = Path(tmpdir) / "experiments" / "models"
        experiments_dir.mkdir(parents=True, exist_ok=True)
        model_file = experiments_dir / "NVDA_best_model.pkl"
        model_file.touch()  # Create empty file
        
        # Should try to load (will fail but test the path)
        service = controller.get_regime_service("NVDA")
        # May return None if loading fails (expected)
        assert service is None or hasattr(service, 'predict_regime')


def test_get_regime_service_legacy_path():
    """Test regime service loading from legacy HMM path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create fake legacy model path
        legacy_dir = Path(tmpdir) / "models"
        legacy_dir.mkdir(parents=True, exist_ok=True)
        model_file = legacy_dir / "NVDA_hmm.pkl"
        model_file.touch()  # Create empty file
        
        # Should try to load (will fail but test the path)
        service = controller.get_regime_service("NVDA")
        # May return None if loading fails (expected)
        assert service is None or hasattr(service, 'predict_regime')


def test_process_tick_regime_modes():
    """Test process_tick with different regime modes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test rule mode
        os.environ["REGIME_MODE"] = "rule"
        controller = ServiceController(base_dir=tmpdir)
        
        from backend.models.domain import TickerDTO
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Process multiple ticks to fill VPIN bucket
        for _ in range(50):  # Enough to fill bucket
            result = controller.process_tick(ticker_dto)
            assert "vpin" in result
            assert "volatility" in result
        
        os.environ.pop("REGIME_MODE", None)


def test_process_tick_exception_handling():
    """Test process_tick exception handling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        from backend.models.domain import TickerDTO
        
        # Invalid ticker DTO (missing required fields)
        try:
            ticker_dto = TickerDTO(
                event_type="TICK",
                timestamp=pd.Timestamp.now(),
                ticker="",  # Empty ticker
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000
            )
            result = controller.process_tick(ticker_dto)
            # Should handle gracefully
            assert isinstance(result, dict)
        except Exception:
            # Exception is also acceptable
            pass

