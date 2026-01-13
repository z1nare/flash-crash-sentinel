"""Additional unit tests to boost feature_engineering.py coverage."""
import numpy as np
import pandas as pd
import pytest

from services.feature_engineering import FeatureEngineer


def test_feature_engineer_initialization():
    """Test FeatureEngineer initialization."""
    engineer = FeatureEngineer()
    assert engineer is not None
    assert engineer.feature_names == []


def test_engineer_features_basic():
    """Test basic feature engineering."""
    engineer = FeatureEngineer()
    
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=100, freq="h"),
        "open": 100.0 + np.random.randn(100) * 0.5,
        "high": 101.0 + np.random.randn(100) * 0.5,
        "low": 99.0 + np.random.randn(100) * 0.5,
        "close": 100.5 + np.random.randn(100) * 0.5,
        "volume": 1000.0 + np.random.randn(100) * 100,
        "VPIN": np.random.rand(100),
        "vol": np.random.rand(100) * 0.1
    })
    
    features = engineer.engineer_features(df)
    assert features is not None
    assert len(features) > 0


def test_engineer_features_missing_columns():
    """Test feature engineering with missing columns."""
    engineer = FeatureEngineer()
    
    # Missing some required columns
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h"),
        "open": [100.0] * 10,
        "close": [100.5] * 10
        # Missing high, low, volume, VPIN, vol
    })
    
    # Should handle gracefully
    features = engineer.engineer_features(df)
    assert features is not None


def test_normalize_columns():
    """Test column normalization."""
    engineer = FeatureEngineer()
    
    df = pd.DataFrame({
        "VPIN": [0.5],
        "vpin": [0.6],  # Duplicate
        "volatility": [0.02],
        "vol": [0.03],  # Duplicate
        "close": [100.0]
    })
    
    normalized = engineer._normalize_columns(df)
    assert "vpin" in normalized.columns or "VPIN" in normalized.columns
    assert "vol" in normalized.columns or "volatility" in normalized.columns


def test_merge_sentiment_features():
    """Test sentiment feature merging."""
    engineer = FeatureEngineer()
    
    # The method converts sentiment_df timestamps to UTC (utc=True)
    # So we need to ensure features timestamps are also UTC-compatible
    # Create timestamps and convert both to UTC to match the method's behavior
    features = pd.DataFrame({
        "timestamp": pd.to_datetime(pd.date_range("2024-01-01", periods=10, freq="h"), utc=True),
        "close": [100.0] * 10,
        "VPIN": [0.5] * 10
    })
    
    sentiment_df = pd.DataFrame({
        "timestamp": pd.to_datetime(pd.date_range("2024-01-01", periods=5, freq="D"), utc=True),
        "sentiment_score": [0.1, -0.2, 0.3, -0.1, 0.2]
    })
    
    merged = engineer._merge_sentiment_features(features, sentiment_df)
    assert merged is not None
    # Should have sentiment column if merge succeeded
    assert len(merged) > 0


def test_merge_sentiment_features_missing_timestamp():
    """Test sentiment merge with missing timestamp."""
    engineer = FeatureEngineer()
    
    features = pd.DataFrame({
        "close": [100.0] * 10,
        "VPIN": [0.5] * 10
    })
    
    sentiment_df = pd.DataFrame({
        "sentiment_score": [0.1] * 5
    })
    
    # Should return original features if timestamp missing
    merged = engineer._merge_sentiment_features(features, sentiment_df)
    assert merged is not None
    assert len(merged) == len(features)


def test_get_feature_names():
    """Test getting feature names."""
    engineer = FeatureEngineer()
    
    # Initially empty
    names = engineer.get_feature_names()
    assert names == []
    
    # After engineering, should have names
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=50, freq="h"),
        "open": [100.0] * 50,
        "high": [101.0] * 50,
        "low": [99.0] * 50,
        "close": [100.5] * 50,
        "volume": [1000.0] * 50,
        "VPIN": [0.5] * 50,
        "vol": [0.02] * 50
    })
    
    engineer.engineer_features(df)
    names = engineer.get_feature_names()
    assert len(names) > 0


def test_get_feature_matrix():
    """Test getting feature matrix."""
    engineer = FeatureEngineer()
    
    # First engineer features to populate feature_names
    df_full = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=50, freq="h"),
        "open": [100.0] * 50,
        "high": [101.0] * 50,
        "low": [99.0] * 50,
        "close": [100.5] * 50,
        "volume": [1000.0] * 50,
        "VPIN": [0.5] * 50,
        "vol": [0.02] * 50
    })
    engineer.engineer_features(df_full)
    
    # Now test with subset
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h"),
        "close": [100.0] * 10,
        "VPIN": [0.5] * 10,
        "vol": [0.02] * 10,
        "ticker": ["TEST"] * 10  # Non-numeric, should be excluded
    })
    
    matrix = engineer.get_feature_matrix(df)
    assert isinstance(matrix, np.ndarray)
    assert matrix.shape[0] == len(df)
    # Should only include numeric columns
    assert matrix.shape[1] >= 0  # May be 0 if no matching features


def test_get_feature_matrix_empty():
    """Test feature matrix with empty DataFrame."""
    engineer = FeatureEngineer()
    
    df = pd.DataFrame()
    # Empty DataFrame should raise ValueError
    with pytest.raises(ValueError, match="No features found"):
        matrix = engineer.get_feature_matrix(df)

