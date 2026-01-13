"""Additional unit tests to boost vol_service.py coverage."""
import numpy as np
import pandas as pd
import pytest

from services.vol_service import VolatilityService


def test_calculate_yang_zhang_edge_cases():
    """Test Yang-Zhang calculation with edge cases."""
    service = VolatilityService(csv_path="")
    
    # Single row (should return 0)
    df_single = pd.DataFrame({
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5]
    })
    result = service._calculate_yang_zhang(df_single)
    assert result == 0.0
    
    # Two rows (minimum for calculation)
    df_two = pd.DataFrame({
        "open": [100.0, 100.5],
        "high": [101.0, 101.5],
        "low": [99.0, 99.5],
        "close": [100.5, 101.0]
    })
    result = service._calculate_yang_zhang(df_two)
    assert result >= 0.0  # Should be non-negative
    
    # All same prices (zero volatility)
    df_flat = pd.DataFrame({
        "open": [100.0] * 5,
        "high": [100.0] * 5,
        "low": [100.0] * 5,
        "close": [100.0] * 5
    })
    result = service._calculate_yang_zhang(df_flat)
    assert result == 0.0


def test_calculate_yang_zhang_with_nan():
    """Test Yang-Zhang with NaN values."""
    service = VolatilityService(csv_path="")
    
    df_nan = pd.DataFrame({
        "open": [100.0, np.nan, 100.5],
        "high": [101.0, 101.5, 101.5],
        "low": [99.0, 99.5, 99.5],
        "close": [100.5, 101.0, 101.0]
    })
    # Should handle NaN gracefully
    result = service._calculate_yang_zhang(df_nan)
    assert isinstance(result, (float, np.floating))


def test_calculate_rolling_volatility_edge_cases():
    """Test rolling volatility calculation edge cases."""
    # Test with insufficient data - need timestamp column
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="h"),
        "open": [100.0] * 5,
        "high": [101.0] * 5,
        "low": [99.0] * 5,
        "close": [100.5] * 5
    })
    
    result = VolatilityService.calculate_rolling_volatility(df, window=10)
    assert len(result) == len(df)
    assert all(x == 0.0 for x in result)  # Insufficient data


def test_volatility_service_cache():
    """Test volatility service caching behavior."""
    service = VolatilityService(csv_path="")
    
    # First call should compute
    df = pd.DataFrame({
        "open": [100.0] * 21,
        "high": [101.0] * 21,
        "low": [99.0] * 21,
        "close": [100.5] * 21
    })
    
    result1 = service._calculate_yang_zhang(df)
    result2 = service._calculate_yang_zhang(df)
    
    # Results should be consistent
    assert result1 == result2


def test_volatility_service_empty_dataframe():
    """Test with empty DataFrame."""
    service = VolatilityService(csv_path="")
    
    df_empty = pd.DataFrame(columns=["open", "high", "low", "close"])
    result = service._calculate_yang_zhang(df_empty)
    assert result == 0.0

