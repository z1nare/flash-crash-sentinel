"""
Hidden Markov Model (HMM) for Market Regime Detection

Trains a Gaussian HMM on VPIN and Volatility data to classify market states:
- Regime 0: Low Vol / Normal
- Regime 1: High Vol / Correction
- Regime 2: Crash / Liquidity Crisis

Based on the ActionPlan.md specification.
"""
import numpy as np
import pandas as pd
from typing import Optional, Tuple, Dict
from pathlib import Path
import pickle
import os

try:
    from hmmlearn import hmm
except ImportError:
    print("⚠️  Warning: hmmlearn not installed. Install with: pip install hmmlearn")
    hmm = None

# Regime labels for interpretation
REGIME_LABELS = {
    0: "Low Vol / Normal",
    1: "High Vol / Correction",
    2: "Crash / Liquidity Crisis"
}

class HMMRegimeService:
    """
    Service for detecting market regimes using Hidden Markov Models.
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize HMM Regime Service.
        
        Args:
            model_path: Path to saved HMM model file (optional). If not provided,
                       model will be trained on first use.
        """
        self.model: Optional[hmm.GaussianHMM] = None
        self.model_path = model_path
        self.n_states = 3  # 3 regimes: Normal, Correction, Crash
        self.features = ['vpin', 'volatility']  # Features for HMM
        
        # Load saved model if path provided
        if model_path and os.path.exists(model_path):
            self._load_model(model_path)
    
    def train_model(
        self, 
        data: pd.DataFrame,
        n_states: int = 3,
        n_iter: int = 100,
        random_state: int = 42
    ) -> None:
        """
        Train a Gaussian HMM on historical VPIN and Volatility data.
        
        Args:
            data: DataFrame with columns ['vpin', 'volatility'] and 'timestamp'
            n_states: Number of hidden states (regimes)
            n_iter: Maximum number of EM iterations
            random_state: Random seed for reproducibility
        """
        if hmm is None:
            raise ImportError("hmmlearn package is required. Install with: pip install hmmlearn")
        
        print(f"[HMM] Training model with {n_states} states...")
        
        # Prepare features (VPIN and Volatility)
        features = []
        for col in self.features:
            if col not in data.columns:
                # Try alternative column names
                if col == 'vpin' and 'VPIN' in data.columns:
                    features.append(data['VPIN'].values.reshape(-1, 1))
                elif col == 'volatility' and 'vol' in data.columns:
                    features.append(data['vol'].values.reshape(-1, 1))
                else:
                    raise ValueError(f"Required column '{col}' not found in data")
            else:
                features.append(data[col].values.reshape(-1, 1))
        
        # Combine features into 2D array (n_samples, n_features)
        X = np.hstack(features)
        
        # Filter out NaN and infinite values
        valid_mask = np.isfinite(X).all(axis=1)
        X = X[valid_mask]
        
        if len(X) < 100:
            raise ValueError(f"Insufficient data for training: {len(X)} samples (need at least 100)")
        
        print(f"[HMM] Using {len(X):,} valid samples for training")
        
        # Initialize and train HMM
        self.model = hmm.GaussianHMM(
            n_components=n_states,
            covariance_type="full",
            n_iter=n_iter,
            random_state=random_state
        )
        
        self.model.fit(X)
        self.n_states = n_states
        
        # Analyze trained model to label regimes
        self._analyze_regimes(X)
        
        print(f"[HMM] ✅ Model trained successfully")
    
    def _analyze_regimes(self, X: np.ndarray) -> None:
        """
        Analyze the trained HMM to assign labels to regimes.
        Higher VPIN and Volatility typically indicates higher risk regime.
        """
        # Predict states for training data
        states = self.model.predict(X)
        
        # Calculate average VPIN and Volatility per regime
        regime_stats = {}
        for state in range(self.n_states):
            mask = states == state
            if mask.sum() > 0:
                avg_vpin = X[mask, 0].mean()
                avg_vol = X[mask, 1].mean()
                regime_stats[state] = {
                    'avg_vpin': avg_vpin,
                    'avg_volatility': avg_vol,
                    'count': mask.sum()
                }
        
        # Sort regimes by risk (average VPIN * Volatility)
        sorted_regimes = sorted(
            regime_stats.items(),
            key=lambda x: x[1]['avg_vpin'] * x[1]['avg_volatility']
        )
        
        print(f"[HMM] Regime Statistics:")
        for state, stats in sorted_regimes:
            print(f"  State {state}: VPIN={stats['avg_vpin']:.4f}, Vol={stats['avg_volatility']:.4f}, Count={stats['count']}")
    
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
            raise ValueError("Model not trained or loaded. Call train_model() first or load a saved model.")
        
        # Prepare feature vector
        X = np.array([[vpin, volatility]])
        
        # Check for invalid values
        if not np.isfinite(X).all():
            print(f"[HMM] Warning: Invalid input (VPIN={vpin}, Vol={volatility}), returning regime 0")
            return 0, 0.0
        
        # Predict state
        state = self.model.predict(X)[0]
        
        # Get probabilities for all states
        log_probs = self.model.score_samples(X)[1]
        probs = np.exp(log_probs[0])
        confidence = float(probs[state])
        
        return int(state), confidence
    
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
            raise ValueError("Model not trained or loaded.")
        
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
        
        # Predict states
        states = self.model.predict(X_valid)
        
        # Get probabilities
        log_probs = self.model.score_samples(X_valid)[1]
        probs = np.exp(log_probs)
        confidences = probs[np.arange(len(states)), states]
        
        # Create result DataFrame
        result = data.copy()
        result['regime'] = np.nan
        result['regime_confidence'] = np.nan
        
        result.loc[valid_mask, 'regime'] = states
        result.loc[valid_mask, 'regime_confidence'] = confidences
        
        return result
    
    def save_model(self, file_path: str) -> None:
        """Save trained model to disk."""
        if self.model is None:
            raise ValueError("No model to save. Train or load a model first.")
        
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
        
        with open(file_path, 'wb') as f:
            pickle.dump(self.model, f)
        
        print(f"[HMM] Model saved to {file_path}")
    
    def _load_model(self, file_path: str) -> None:
        """Load saved model from disk."""
        if hmm is None:
            raise ImportError("hmmlearn package is required.")
        
        with open(file_path, 'rb') as f:
            self.model = pickle.load(f)
        
        self.n_states = self.model.n_components
        print(f"[HMM] Model loaded from {file_path}")
    
    def get_regime_label(self, regime: int) -> str:
        """Get human-readable label for a regime."""
        return REGIME_LABELS.get(regime, f"Unknown ({regime})")
    
    @staticmethod
    def train_from_csv(
        csv_path: str,
        ticker: str,
        output_model_path: Optional[str] = None,
        n_states: int = 3
    ) -> 'HMMRegimeService':
        """
        Convenience method to train HMM directly from a CSV file.
        
        Args:
            csv_path: Path to CSV file with VPIN and vol columns
            ticker: Ticker symbol (for filtering)
            output_model_path: Optional path to save trained model
            n_states: Number of hidden states
            
        Returns:
            Trained HMMRegimeService instance
        """
        # Load data
        df = pd.read_csv(csv_path, low_memory=False)
        
        # Parse timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert(None)
        
        # Filter for ticker
        if 'ticker' in df.columns:
            df = df[df['ticker'].str.upper() == ticker.upper()].copy()
        
        # Prepare features (use existing column names)
        if 'VPIN' in df.columns:
            df['vpin'] = pd.to_numeric(df['VPIN'], errors='coerce')
        if 'vol' in df.columns:
            df['volatility'] = pd.to_numeric(df['vol'], errors='coerce')
        
        # Filter out rows with missing features
        df = df.dropna(subset=['vpin', 'volatility'])
        
        # Filter out zero values (not meaningful for regime detection)
        df = df[(df['vpin'] > 0) & (df['volatility'] > 0)].copy()
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        if len(df) < 100:
            raise ValueError(f"Insufficient data for training: {len(df)} rows (need at least 100)")
        
        # Create and train service
        service = HMMRegimeService()
        service.train_model(df, n_states=n_states)
        
        # Save if path provided
        if output_model_path:
            service.save_model(output_model_path)
        
        return service

