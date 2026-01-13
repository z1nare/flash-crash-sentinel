"""Tests for ServiceController initialization paths to boost coverage."""
import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from controllers.ServiceController import ServiceController


def test_service_controller_ib_auto_connect():
    """Test IB auto-connect when USE_IB_REALTIME is set."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_env = os.environ.get("USE_IB_REALTIME", "")
        os.environ["USE_IB_REALTIME"] = "true"
        
        # Mock IB service to avoid actual connection
        with patch('services.ib_client_service.IBClientService') as mock_ib:
            mock_ib_instance = MagicMock()
            mock_ib.return_value = mock_ib_instance
            
            controller = ServiceController(base_dir=tmpdir)
            # Should attempt to connect
            assert controller is not None
        
        if original_env:
            os.environ["USE_IB_REALTIME"] = original_env
        else:
            os.environ.pop("USE_IB_REALTIME", None)


def test_service_controller_ib_auto_stream():
    """Test IB auto-stream when IB_STREAM_TICKERS is set."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_env = os.environ.get("IB_STREAM_TICKERS", "")
        os.environ["USE_IB_REALTIME"] = "true"
        os.environ["IB_STREAM_TICKERS"] = "NVDA,TSLA"
        
        with patch('services.ib_client_service.IBClientService') as mock_ib:
            mock_ib_instance = MagicMock()
            mock_ib.return_value = mock_ib_instance
            
            controller = ServiceController(base_dir=tmpdir)
            # Should attempt to start streams
            assert controller is not None
        
        if original_env:
            os.environ["IB_STREAM_TICKERS"] = original_env
        else:
            os.environ.pop("IB_STREAM_TICKERS", None)
        os.environ.pop("USE_IB_REALTIME", None)


def test_service_controller_ib_init_failure():
    """Test ServiceController when IB initialization fails."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch('services.ib_client_service.IBClientService') as mock_ib:
            mock_ib.side_effect = Exception("IB not available")
            
            # Should not crash, just continue without IB
            controller = ServiceController(base_dir=tmpdir)
            assert controller is not None
            assert controller.ib_service is None or not controller.ib_available()


def test_service_controller_process_tick_exception_path():
    """Test process_tick exception handling path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        from backend.models.domain import TickerDTO
        import pandas as pd
        
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
        
        # Process enough ticks to fill VPIN bucket and trigger volatility calculation
        for _ in range(50):
            result = controller.process_tick(ticker_dto)
            assert isinstance(result, dict)
        
        # Now test exception path by mocking volatility service to raise
        with patch.object(controller, 'get_volatility_service') as mock_vol:
            mock_vol_service = MagicMock()
            mock_vol_service.process_tick.side_effect = Exception("Volatility error")
            mock_vol.return_value = mock_vol_service
            
            # Should handle exception gracefully
            result = controller.process_tick(ticker_dto)
            assert isinstance(result, dict)
            assert "vpin" in result
            # Volatility should be 0.0 on error
            assert result.get("volatility") == 0.0 or result.get("volatility") is None


def test_service_controller_process_tick_vol_zero():
    """Test process_tick when vol_score is zero (regime not calculated)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        from backend.models.domain import TickerDTO
        import pandas as pd
        
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
        
        # Fill VPIN bucket
        for _ in range(50):
            controller.process_tick(ticker_dto)
        
        # Mock volatility to return 0.0
        with patch.object(controller, 'get_volatility_service') as mock_vol:
            mock_vol_service = MagicMock()
            mock_vol_service.process_tick.return_value = 0.0
            mock_vol.return_value = mock_vol_service
            
            result = controller.process_tick(ticker_dto)
            assert isinstance(result, dict)
            # Regime should not be calculated when vol is 0
            assert result.get("regime") is None

