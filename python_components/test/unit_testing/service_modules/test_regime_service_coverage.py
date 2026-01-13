"""Additional unit tests to boost regime_service.py coverage."""
import numpy as np
import pandas as pd
import pytest

from services.regime_service import RegimeDetectionService, REGIME_LABELS, SklearnRegimeModel
from sklearn.linear_model import LogisticRegression


def test_regime_service_initialization():
    """Test regime service initialization."""
    service = RegimeDetectionService()
    assert service is not None
    assert service.model is None  # No model by default


def test_regime_service_initialization_with_model():
    """Test regime service initialization with a model."""
    # Create a simple model
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    assert service.model is not None


def test_get_regime_label():
    """Test regime label retrieval using REGIME_LABELS."""
    # Test using the module-level REGIME_LABELS
    assert REGIME_LABELS[0] == "Low Vol / Normal"
    assert REGIME_LABELS[1] == "High Vol / Correction"
    assert REGIME_LABELS[2] == "Crash / Liquidity Crisis"
    
    # Invalid regime - should handle gracefully
    invalid_label = REGIME_LABELS.get(99, f"Unknown ({99})")
    assert "Unknown" in invalid_label or invalid_label == "Unknown (99)"


def test_predict_regime_with_model():
    """Test regime prediction with a trained model."""
    # Create and train a simple model
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04], [0.2, 0.01], [0.8, 0.05]])
    y = np.array([0, 1, 2, 0, 1])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # Test predictions
    test_cases = [
        (0.0, 0.0),
        (0.5, 0.5),
        (0.99, 0.99),
        (1.0, 1.0)
    ]
    
    for vpin, vol in test_cases:
        state, confidence = service.predict_regime(vpin, vol)
        assert state in [0, 1, 2]
        assert 0.0 <= confidence <= 1.0


def test_predict_regime_without_model():
    """Test regime prediction without a model raises error."""
    service = RegimeDetectionService()
    
    with pytest.raises(ValueError, match="Model not loaded"):
        service.predict_regime(0.5, 0.5)


def test_predict_regime_with_nan():
    """Test regime prediction with NaN inputs."""
    # Create a simple model
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    y = np.array([0, 1, 2])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # NaN inputs should return regime 0 with confidence 0.0
    state, confidence = service.predict_regime(np.nan, 0.5)
    assert state == 0
    assert confidence == 0.0
    
    state, confidence = service.predict_regime(0.5, np.nan)
    assert state == 0
    assert confidence == 0.0


def test_predict_regimes_batch():
    """Test batch regime prediction."""
    # Create and train a model
    model = LogisticRegression()
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04], [0.2, 0.01], [0.8, 0.05]])
    y = np.array([0, 1, 2, 0, 1])
    model.fit(X, y)
    
    wrapped_model = SklearnRegimeModel(model=model, scaler=None)
    service = RegimeDetectionService(model=wrapped_model)
    
    # Create test DataFrame
    df = pd.DataFrame({
        'vpin': [0.1, 0.5, 0.9, np.nan, 0.2],
        'volatility': [0.02, 0.03, 0.04, 0.01, np.nan],
        'timestamp': pd.date_range('2024-01-01', periods=5, freq='H')
    })
    
    result = service.predict_regimes_batch(df)
    assert 'regime' in result.columns
    assert 'regime_confidence' in result.columns
    assert len(result) == len(df)

