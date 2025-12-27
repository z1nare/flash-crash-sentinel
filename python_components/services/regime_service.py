"""
Modular Regime Detection Service

This service provides a unified interface for regime detection using any model.
Models can be swapped via a plugin-style architecture.

Supported models:
- Logistic Regression (baseline)
- XGBoost Classifier
- Random Forest Classifier
- Multinomial Naive Bayes
- Hidden Markov Model
- Any sklearn-compatible classifier
"""
import numpy as np
import pandas as pd
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import pickle
import os
from abc import ABC, abstractmethod

# Regime labels for interpretation
REGIME_LABELS = {
    0: "Low Vol / Normal",
    1: "High Vol / Correction",
    2: "Crash / Liquidity Crisis"
}

class RegimeModelInterface(ABC):
    """
    Abstract base class for regime detection models.
    All regime models must implement this interface.
    """
    
    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> 'RegimeModelInterface':
        """Train the model on features X and target y."""
        pass
    
    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict regime states for given features."""
        pass
    
    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return probability distribution over regimes."""
        pass
    
    @abstractmethod
    def save(self, file_path: str) -> None:
        """Save the trained model to disk."""
        pass
    
    @classmethod
    @abstractmethod
    def load(cls, file_path: str) -> 'RegimeModelInterface':
        """Load a saved model from disk."""
        pass

class SklearnRegimeModel(RegimeModelInterface):
    """
    Wrapper for sklearn-compatible models.
    """
    
    def __init__(self, model, scaler=None):
        """
        Initialize with a sklearn model.
        
        Args:
            model: Sklearn-compatible classifier
            scaler: Optional scaler for feature normalization
        """
        self.model = model
        self.scaler = scaler
        self.needs_scaling = scaler is not None
    
    def fit(self, X: np.ndarray, y: np.ndarray) -> 'SklearnRegimeModel':
        """Train the model."""
        X_scaled = self.scaler.fit_transform(X) if self.scaler else X
        self.model.fit(X_scaled, y)
        return self
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict regime states."""
        X_scaled = self.scaler.transform(X) if self.scaler else X
        return self.model.predict(X_scaled)
    
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return probability distribution."""
        X_scaled = self.scaler.transform(X) if self.scaler else X
        if hasattr(self.model, 'predict_proba'):
            return self.model.predict_proba(X_scaled)
        else:
            # For models without predict_proba, return one-hot encoding
            predictions = self.model.predict(X_scaled)
            n_classes = len(np.unique(predictions))
            proba = np.zeros((len(predictions), n_classes))
            for i, pred in enumerate(predictions):
                proba[i, int(pred)] = 1.0
            return proba
    
    def save(self, file_path: str) -> None:
        """Save model and scaler."""
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'needs_scaling': self.needs_scaling
        }
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
        with open(file_path, 'wb') as f:
            pickle.dump(model_data, f)
    
    @classmethod
    def load(cls, file_path: str) -> 'SklearnRegimeModel':
        """Load model and scaler."""
        with open(file_path, 'rb') as f:
            model_data = pickle.load(f)
        return cls(
            model=model_data['model'],
            scaler=model_data.get('scaler')
        )

class RegimeDetectionService:
    """
    Unified service for regime detection using any compatible model.
    """
    
    def __init__(self, model: Optional[RegimeModelInterface] = None, model_path: Optional[str] = None):
        """
        Initialize Regime Detection Service.
        
        Args:
            model: Optional pre-trained model instance
            model_path: Optional path to saved model file
        """
        self.model: Optional[RegimeModelInterface] = None
        self.model_path = model_path
        self.features = ['vpin', 'volatility']
        
        # Load model if path provided
        if model_path and os.path.exists(model_path):
            self._load_model(model_path)
        elif model is not None:
            self.model = model
    
    def predict_regime(
        self, 
        vpin: float, 
        volatility: float
    ) -> Tuple[int, float]:
        """
        Predict the current market regime given VPIN and Volatility.
        
        Args:
            vpin: Current VPIN score
            volatility: Current volatility value
            
        Returns:
            Tuple of (regime_state, confidence)
            - regime_state: 0 (Normal), 1 (Correction), or 2 (Crash)
            - confidence: Probability of the predicted state
        """
        if self.model is None:
            raise ValueError("Model not loaded. Load a model first or provide model_path.")
        
        # Prepare feature vector
        X = np.array([[vpin, volatility]])
        
        # Check for invalid values
        if not np.isfinite(X).all():
            print(f"[REGIME] Warning: Invalid input (VPIN={vpin}, Vol={volatility}), returning regime 0")
            return 0, 0.0
        
        try:
            # Predict state
            state = self.model.predict(X)[0]
            state = int(state)
            
            # Get probabilities
            probs = self.model.predict_proba(X)[0]
            confidence = float(probs[state])
            
            # Ensure state is in valid range
            if state < 0 or state > 2:
                state = 0
                confidence = 0.0
            
            return state, confidence
            
        except Exception as e:
            print(f"[REGIME] Error predicting regime: {e}")
            return 0, 0.0
    
    def predict_regimes_batch(
        self,
        data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Predict regimes for a batch of data points.
        
        Args:
            data: DataFrame with columns ['vpin', 'volatility'] and optionally 'timestamp'
            
        Returns:
            DataFrame with added 'regime' and 'regime_confidence' columns
        """
        if self.model is None:
            raise ValueError("Model not loaded.")
        
        # Prepare features
        if 'vpin' not in data.columns and 'VPIN' in data.columns:
            data = data.copy()
            data['vpin'] = data['VPIN']
        if 'volatility' not in data.columns and 'vol' in data.columns:
            data = data.copy()
            data['volatility'] = data['vol']
        
        # Prepare feature matrix
        features = []
        for col in self.features:
            if col not in data.columns:
                raise ValueError(f"Column '{col}' not found in data")
            features.append(data[col].values)
        
        X = np.column_stack(features)
        
        # Filter invalid rows
        valid_mask = np.isfinite(X).all(axis=1)
        X_valid = X[valid_mask]
        
        if len(X_valid) == 0:
            data['regime'] = None
            data['regime_confidence'] = None
            return data
        
        # Predict states
        states = self.model.predict(X_valid)
        states = states.astype(int)
        
        # Get probabilities
        probas = self.model.predict_proba(X_valid)
        confidences = probas[np.arange(len(states)), states]
        
        # Create result DataFrame
        result = data.copy()
        result['regime'] = None
        result['regime_confidence'] = None
        
        result.loc[valid_mask, 'regime'] = states
        result.loc[valid_mask, 'regime_confidence'] = confidences
        
        return result
    
    def _load_model(self, model_path: str) -> None:
        """Load saved model from disk."""
        try:
            # Try loading as SklearnRegimeModel first
            self.model = SklearnRegimeModel.load(model_path)
            print(f"[REGIME] Model loaded from {model_path}")
        except Exception as e:
            print(f"[REGIME] Error loading model: {e}")
            raise
    
    def save_model(self, file_path: str) -> None:
        """Save trained model to disk."""
        if self.model is None:
            raise ValueError("No model to save. Train or load a model first.")
        
        self.model.save(file_path)
        print(f"[REGIME] Model saved to {file_path}")
    
    def get_regime_label(self, regime: int) -> str:
        """Get human-readable label for a regime."""
        return REGIME_LABELS.get(regime, f"Unknown ({regime})")
    
    @staticmethod
    def load_from_experiment_results(
        ticker: str,
        experiments_dir: Optional[Path] = None
    ) -> 'RegimeDetectionService':
        """
        Load the best model from experiment results.
        
        Args:
            ticker: Stock ticker symbol
            experiments_dir: Path to experiments directory (default: ./experiments/regime_detection/models)
            
        Returns:
            RegimeDetectionService instance with loaded model
        """
        if experiments_dir is None:
            experiments_dir = Path(__file__).parent.parent / "experiments" / "regime_detection" / "models"
        
        model_path = experiments_dir / f"{ticker}_best_model.pkl"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Best model not found for {ticker} at {model_path}. Run experiments first.")
        
        # Load metadata
        metadata_path = experiments_dir / f"{ticker}_metadata.json"
        metadata = {}
        if metadata_path.exists():
            import json
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        
        service = RegimeDetectionService(model_path=str(model_path))
        
        print(f"[REGIME] Loaded {metadata.get('model_name', 'unknown')} model for {ticker}")
        print(f"  Test Accuracy: {metadata.get('test_accuracy', 'N/A')}")
        print(f"  Test F1: {metadata.get('test_f1', 'N/A')}")
        
        return service

