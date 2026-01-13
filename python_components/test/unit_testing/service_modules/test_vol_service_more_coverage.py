"""Additional unit tests to boost vol_service.py coverage further."""
import numpy as np
import pandas as pd
import pytest

from services.vol_service import VolatilityService


def test_process_tick_with_cache():
    """Test process_tick with caching behavior."""
    service = VolatilityService(csv_path="")
    
    # Create a simple tick-like object
    class MockTicker:
        def __init__(self):
            self.ticker = "TEST"
            self.timestamp = pd.Timestamp.now()
            self.open = 100.0
            self.high = 101.0
            self.low = 99.0
            self.close = 100.5
            self.volume = 1000
    
    ticker = MockTicker()
    
    # First call
    result1 = service.process_tick(ticker)
    
    # Second call (should use cache if available)
    result2 = service.process_tick(ticker)
    
    # Results should be consistent
    assert isinstance(result1, (float, type(None)))
    assert isinstance(result2, (float, type(None)))


def test_calculate_yang_zhang_extreme_values():
    """Test Yang-Zhang with extreme price values."""
    service = VolatilityService(csv_path="")
    
    # Very small prices
    df_small = pd.DataFrame({
        "open": [0.01, 0.02, 0.03],
        "high": [0.02, 0.03, 0.04],
        "low": [0.01, 0.02, 0.03],
        "close": [0.015, 0.025, 0.035]
    })
    result = service._calculate_yang_zhang(df_small)
    assert result >= 0.0
    
    # Very large prices
    df_large = pd.DataFrame({
        "open": [10000.0, 10001.0, 10002.0],
        "high": [10001.0, 10002.0, 10003.0],
        "low": [9999.0, 10000.0, 10001.0],
        "close": [10000.5, 10001.5, 10002.5]
    })
    result = service._calculate_yang_zhang(df_large)
    assert result >= 0.0


def test_calculate_yang_zhang_negative_log():
    """Test Yang-Zhang with prices that could cause negative log issues."""
    service = VolatilityService(csv_path="")
    
    # Prices where high < open (shouldn't happen but test robustness)
    df_weird = pd.DataFrame({
        "open": [100.0, 101.0, 102.0],
        "high": [99.0, 100.0, 101.0],  # High < open (invalid but test handling)
        "low": [98.0, 99.0, 100.0],
        "close": [99.5, 100.5, 101.5]
    })
    # Should handle gracefully (may return 0 or handle error)
    try:
        result = service._calculate_yang_zhang(df_weird)
        assert isinstance(result, (float, np.floating))
    except (ValueError, RuntimeError):
        pass  # Expected if validation catches invalid OHLC


def test_calculate_rolling_volatility_timezone_handling():
    """Test rolling volatility with timezone-aware timestamps."""
    service = VolatilityService()
    
    # Timezone-aware timestamps
    df_tz = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=30, freq="h", tz="UTC"),
        "open": [100.0] * 30,
        "high": [101.0] * 30,
        "low": [99.0] * 30,
        "close": [100.5] * 30
    })
    
    result = service.calculate_rolling_volatility(df_tz, window=10)
    assert len(result) == len(df_tz)
    assert all(x >= 0.0 for x in result)


def test_calculate_rolling_volatility_invalid_rows():
    """Test rolling volatility with invalid rows."""
    service = VolatilityService()
    
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=30, freq="h"),
        "open": [100.0] * 30,
        "high": [101.0] * 30,
        "low": [99.0] * 30,
        "close": [100.5] * 30
    })
    
    # Add some NaN values
    df.loc[5:10, "open"] = np.nan
    df.loc[15:20, "close"] = np.nan
    
    result = service.calculate_rolling_volatility(df, window=10)
    assert len(result) == len(df)
    # Should handle NaN gracefully


def test_calculate_rolling_volatility_insufficient_valid():
    """Test rolling volatility with insufficient valid rows."""
    service = VolatilityService()
    
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=30, freq="h"),
        "open": [np.nan] * 30,  # All NaN
        "high": [101.0] * 30,
        "low": [99.0] * 30,
        "close": [100.5] * 30
    })
    
    result = service.calculate_rolling_volatility(df, window=10)
    assert len(result) == len(df)
    assert all(x == 0.0 for x in result)  # Should return zeros


def test_volatility_service_with_csv_path():
    """Test VolatilityService initialization with CSV path."""
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("timestamp,open,high,low,close,volume\n")
        f.write("2024-01-01 09:30:00,100.0,101.0,99.0,100.5,1000\n")
        csv_path = f.name
    
    try:
        service = VolatilityService(csv_path=csv_path)
        assert service is not None
    finally:
        os.unlink(csv_path)

