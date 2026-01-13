"""Tests for VolatilityService Yang-Zhang calculation to boost coverage."""
import numpy as np
import pandas as pd
import pytest

from services.vol_service import VolatilityService


def test_calculate_yang_zhang_full_path():
    """Test full Yang-Zhang calculation path with valid data."""
    service = VolatilityService()
    
    # Create realistic OHLC data with sufficient history
    dates = pd.date_range("2024-01-01", periods=100, freq="h")
    np.random.seed(42)
    
    # Generate realistic price data
    base_price = 100.0
    prices = [base_price]
    for _ in range(99):
        change = np.random.randn() * 0.5
        prices.append(prices[-1] * (1 + change / 100))
    
    df = pd.DataFrame({
        "open": prices,
        "high": [p * (1 + abs(np.random.randn() * 0.01)) for p in prices],
        "low": [p * (1 - abs(np.random.randn() * 0.01)) for p in prices],
        "close": [p * (1 + np.random.randn() * 0.005) for p in prices],
        "timestamp": dates
    })
    
    # Ensure high >= low, high >= open, high >= close, etc.
    df["high"] = df[["open", "close", "high"]].max(axis=1) * 1.001
    df["low"] = df[["open", "close", "low"]].min(axis=1) * 0.999
    
    result = service._calculate_yang_zhang(df)
    # _calculate_yang_zhang returns a scalar float, not an array
    assert isinstance(result, (float, np.floating))
    assert result >= 0.0
    assert np.isfinite(result)


def test_calculate_yang_zhang_insufficient_daily_data():
    """Test Yang-Zhang with insufficient data (less than 2 rows)."""
    service = VolatilityService()
    
    # Only 1 row of data (n < 2)
    df = pd.DataFrame({
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5],
        "timestamp": pd.date_range("2024-01-01", periods=1, freq="h")
    })
    
    result = service._calculate_yang_zhang(df)
    # Should return 0.0 for insufficient data (n < 2)
    assert isinstance(result, (float, np.floating))
    assert result == 0.0


def test_calculate_yang_zhang_date_mapping():
    """Test Yang-Zhang date normalization and mapping back to original timestamps."""
    service = VolatilityService()
    
    # Create data spanning multiple days
    dates = pd.date_range("2024-01-01 09:30:00", periods=50, freq="h")
    df = pd.DataFrame({
        "open": 100.0 + np.random.randn(50) * 0.5,
        "high": 101.0 + np.random.randn(50) * 0.5,
        "low": 99.0 + np.random.randn(50) * 0.5,
        "close": 100.5 + np.random.randn(50) * 0.5,
        "timestamp": dates
    })
    
    # Ensure valid OHLC
    df["high"] = df[["open", "close", "high"]].max(axis=1) * 1.001
    df["low"] = df[["open", "close", "low"]].min(axis=1) * 0.999
    
    result = service._calculate_yang_zhang(df)
    # Should return a scalar volatility value
    assert isinstance(result, (float, np.floating))
    assert result >= 0.0


def test_calculate_yang_zhang_timezone_handling():
    """Test Yang-Zhang with timezone-aware timestamps."""
    service = VolatilityService()
    
    dates = pd.date_range("2024-01-01", periods=30, freq="D", tz="UTC")
    df = pd.DataFrame({
        "open": [100.0] * 30,
        "high": [101.0] * 30,
        "low": [99.0] * 30,
        "close": [100.5] * 30,
        "timestamp": dates
    })
    
    result = service._calculate_yang_zhang(df)
    # _calculate_yang_zhang returns a scalar float, not an array
    assert isinstance(result, (float, np.floating))
    assert result >= 0.0
    assert np.isfinite(result)


def test_calculate_yang_zhang_invalid_timestamps():
    """Test Yang-Zhang with invalid/NaN timestamps."""
    service = VolatilityService()
    
    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    df = pd.DataFrame({
        "open": [100.0] * 30,
        "high": [101.0] * 30,
        "low": [99.0] * 30,
        "close": [100.5] * 30,
        "timestamp": dates
    })
    
    # Add some NaN timestamps
    df.loc[5:10, "timestamp"] = pd.NaT
    
    result = service._calculate_yang_zhang(df)
    assert isinstance(result, (float, np.floating))
    assert result >= 0.0


def test_calculate_yang_zhang_no_valid_rows():
    """Test Yang-Zhang when insufficient rows (less than 2)."""
    service = VolatilityService()
    
    # Only 1 row - insufficient for calculation
    df = pd.DataFrame({
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5],
        "timestamp": [pd.NaT]
    })
    
    result = service._calculate_yang_zhang(df)
    # Should return 0.0 for n < 2
    assert isinstance(result, (float, np.floating))
    assert result == 0.0

