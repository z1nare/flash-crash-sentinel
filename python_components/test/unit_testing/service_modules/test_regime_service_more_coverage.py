"""Additional regime service tests for more coverage."""
import numpy as np
import pandas as pd
import pytest
import tempfile
from pathlib import Path

from services.regime_service import RegimeDetectionService, SklearnRegimeModel, REGIME_LABELS
from sklearn.linear_model import LogisticRegression


def test_get_regime_label():
    """Test get_regime_label method."""
    service = RegimeDetectionService()
    
    assert service.get_regime_label(0) == REGIME_LABELS[0]
    assert service.get_regime_label(1) == REGIME_LABELS[1]
    assert service.get_regime_label(2) == REGIME_LABELS[2]
    
    # Invalid regime
    assert "Unknown" in service.get_regime_label(99)


def test_save_model():
    """Test saving model."""
    # Create and train a model
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04], [0.2, 0.01], [0.8, 0.05]])
    y = np.array([0, 1, 2, 0, 1])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
        model_path = f.name
    
    try:
        service.save_model(model_path)
        assert Path(model_path).exists()
    finally:
        if Path(model_path).exists():
            Path(model_path).unlink()


def test_save_model_no_model():
    """Test saving when no model is loaded."""
    service = RegimeDetectionService()
    
    with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
        model_path = f.name
    
    try:
        with pytest.raises(ValueError, match="No model to save"):
            service.save_model(model_path)
    finally:
        if Path(model_path).exists():
            Path(model_path).unlink()


def test_load_model_error():
    """Test loading model with invalid path."""
    service = RegimeDetectionService()
    
    # Try loading non-existent model
    with pytest.raises((FileNotFoundError, ValueError, Exception)):
        service._load_model("/nonexistent/path/model.pkl")


def test_load_from_experiment_results_not_found():
    """Test loading from experiment results when file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        RegimeDetectionService.load_from_experiment_results("NONEXISTENT")


def test_predict_regime_invalid_state():
    """Test predict_regime with invalid state returned."""
    # Create model that returns invalid state
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    # Mock predict to return invalid state
    original_predict = model.predict
    def mock_predict(X):
        result = original_predict(X)
        result[0] = 99  # Invalid state
        return result
    
    model.predict = mock_predict
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    state, confidence = service.predict_regime(0.5, 0.5)
    # Should clamp invalid state to 0
    assert state == 0
    assert confidence == 0.0


def test_predict_regimes_batch_missing_columns():
    """Test batch prediction with missing columns."""
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # Missing required columns
    df = pd.DataFrame({
        "close": [100.0] * 10
        # Missing vpin and volatility
    })
    
    with pytest.raises(ValueError, match="Column.*not found"):
        service.predict_regimes_batch(df)


def test_predict_regimes_batch_all_invalid():
    """Test batch prediction with all invalid rows."""
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # All NaN values
    df = pd.DataFrame({
        "vpin": [np.nan] * 10,
        "volatility": [np.nan] * 10,
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h")
    })
    
    result = service.predict_regimes_batch(df)
    assert len(result) == len(df)
    assert result["regime"].isna().all()


def test_predict_regimes_batch_column_aliases():
    """Test batch prediction with column aliases (VPIN vs vpin, vol vs volatility)."""
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # Use uppercase/alternative column names
    df = pd.DataFrame({
        "VPIN": [0.5] * 10,
        "vol": [0.02] * 10,  # lowercase vol instead of volatility
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="h")
    })
    
    result = service.predict_regimes_batch(df)
    assert "regime" in result.columns
    assert "regime_confidence" in result.columns

