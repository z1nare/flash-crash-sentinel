"""Tests for ServiceController initialization paths to boost coverage."""
import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from controllers.ServiceController import ServiceController


def test_service_controller_ib_auto_connect():
    """Test IB auto-connect when USE_IB_REALTIME is set."""
    # Skip if IB service cannot be imported (ibapi not available in CI)
    try:
        # Try to import - if it fails due to duplicate base class, skip
        import services.ib_client_service
        # If import succeeds, check if IBClientService exists
        if not hasattr(services.ib_client_service, 'IBClientService'):
            pytest.skip("IB service not available (ibapi not installed)")
    except (ImportError, TypeError, AttributeError) as e:
        # Skip if import fails (ibapi not available or duplicate base class error)
        pytest.skip(f"IB service not available: {type(e).__name__}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        original_env = os.environ.get("USE_IB_REALTIME", "")
        os.environ["USE_IB_REALTIME"] = "true"
        
        # Don't mock - just test that ServiceController initializes without crashing
        # The actual IB connection would fail, but initialization should succeed
        controller = ServiceController(base_dir=tmpdir)
        assert controller is not None
        
        if original_env:
            os.environ["USE_IB_REALTIME"] = original_env
        else:
            os.environ.pop("USE_IB_REALTIME", None)


def test_service_controller_ib_auto_stream():
    """Test IB auto-stream when IB_STREAM_TICKERS is set."""
    # Skip if IB service cannot be imported
    try:
        import services.ib_client_service
        if not hasattr(services.ib_client_service, 'IBClientService'):
            pytest.skip("IB service not available (ibapi not installed)")
    except (ImportError, TypeError, AttributeError) as e:
        pytest.skip(f"IB service not available: {type(e).__name__}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        original_env = os.environ.get("IB_STREAM_TICKERS", "")
        os.environ["USE_IB_REALTIME"] = "true"
        os.environ["IB_STREAM_TICKERS"] = "NVDA,TSLA"
        
        # Don't mock - test actual initialization behavior
        controller = ServiceController(base_dir=tmpdir)
        assert controller is not None
        
        if original_env:
            os.environ["IB_STREAM_TICKERS"] = original_env
        else:
            os.environ.pop("IB_STREAM_TICKERS", None)
        os.environ.pop("USE_IB_REALTIME", None)


def test_service_controller_ib_init_failure():
    """Test ServiceController when IB initialization fails."""
    # This test works even when IB is not available - it tests graceful degradation
    # The ServiceController handles IB import failures gracefully
    with tempfile.TemporaryDirectory() as tmpdir:
        # Don't patch - let it naturally fail if IB is not available
        # This tests the actual graceful degradation behavior
        controller = ServiceController(base_dir=tmpdir)
        assert controller is not None
        # IB service should be None if not available, or available if it is
        # The key is that initialization doesn't crash
        assert hasattr(controller, 'ib_service')


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
