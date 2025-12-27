"""
Advanced Feature Engineering for Regime Detection

Creates comprehensive feature set including:
- VPIN and volatility
- Moving averages and lags
- Momentum indicators
- Volatility of volatility
- Sentiment moving averages
"""
import pandas as pd
import numpy as np
from typing import List, Optional
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import get_service_logger

# Check pandas version for API compatibility
try:
    _PD_HAS_API_TYPES = hasattr(pd.api, 'types')
except:
    _PD_HAS_API_TYPES = False

logger = get_service_logger("feature_engineering")

class FeatureEngineer:
    """Engineer advanced features for regime detection."""
    
    def __init__(self):
        self.feature_names = []
    
    def engineer_features(
        self, 
        df: pd.DataFrame,
        sentiment_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Create comprehensive feature set for regime detection.
        
        Args:
            df: DataFrame with columns: timestamp, vpin/VPIN, volatility/vol, open, high, low, close, volume
            sentiment_df: Optional DataFrame with sentiment scores and timestamps
            
        Returns:
            DataFrame with engineered features
        """
        features = df.copy()
        
        # Normalize column names
        features = self._normalize_columns(features)
        
        # Store metadata columns before dropping them
        metadata_cols = ['event_type', 'ticker', 'regime', 'regime_label', 'timestamp']
        stored_metadata = {}
        for col in metadata_cols:
            if col in features.columns:
                stored_metadata[col] = features[col].copy()
        
        # Drop non-numeric metadata columns that shouldn't be features
        # (We'll keep numeric ones like timestamp for merging, but drop string ones)
        for col in ['event_type', 'ticker', 'regime_label']:  # Drop string columns
            if col in features.columns:
                features = features.drop(columns=[col], errors='ignore')
        
        logger.info(f"Engineering features from {len(features)} rows")
        
        # Ensure we have timestamp index
        if 'timestamp' in features.columns:
            features['timestamp'] = pd.to_datetime(features['timestamp'], errors='coerce', utc=True)
            if features['timestamp'].isna().any():
                features = features.dropna(subset=['timestamp'])
            features = features.sort_values('timestamp').reset_index(drop=True)
        
        # 1. VPIN Features
        if 'vpin' in features.columns:
            features['vpin_ma5'] = features['vpin'].rolling(5, min_periods=1).mean()
            features['vpin_ma20'] = features['vpin'].rolling(20, min_periods=1).mean()
            features['vpin_std'] = features['vpin'].rolling(20, min_periods=1).std()
            features['vpin_lag1'] = features['vpin'].shift(1)
            features['vpin_lag2'] = features['vpin'].shift(2)
            features['vpin_momentum'] = features['vpin'] - features['vpin'].shift(5)
            logger.debug("Added VPIN features")
        
        # 2. Volatility Features
        if 'volatility' in features.columns:
            features['vol_ma5'] = features['volatility'].rolling(5, min_periods=1).mean()
            features['vol_ma20'] = features['volatility'].rolling(20, min_periods=1).mean()
            features['vol_momentum'] = features['volatility'].diff(5)
            features['vol_lag1'] = features['volatility'].shift(1)
            features['vol_lag2'] = features['volatility'].shift(2)
            # Volatility of Volatility (VIX-like)
            features['vol_of_vol'] = features['volatility'].rolling(20, min_periods=1).std()
            logger.debug("Added volatility features")
        
        # 3. Price Features
        if all(col in features.columns for col in ['open', 'high', 'low', 'close']):
            features['price_momentum'] = features['close'].pct_change(5)
            features['price_acceleration'] = features['price_momentum'].diff()
            features['returns'] = features['close'].pct_change()
            features['returns_abs'] = features['returns'].abs()
            features['returns_lag1'] = features['returns'].shift(1)
            features['high_low_spread'] = (features['high'] - features['low']) / features['close']
            logger.debug("Added price features")
        
        # 4. Volume Features
        if 'volume' in features.columns:
            vol_mean = features['volume'].rolling(20, min_periods=1).mean()
            features['volume_spike'] = (features['volume'] / (vol_mean + 1e-10)) - 1.0
            features['volume_trend'] = features['volume'].rolling(5, min_periods=1).mean().pct_change()
            features['volume_ma5'] = features['volume'].rolling(5, min_periods=1).mean()
            features['volume_ma20'] = features['volume'].rolling(20, min_periods=1).mean()
            logger.debug("Added volume features")
        
        # 5. Sentiment Features (if sentiment data provided)
        if sentiment_df is not None:
            features = self._merge_sentiment_features(features, sentiment_df)
        
        # 6. Interaction Features
        if 'vpin' in features.columns and 'volatility' in features.columns:
            features['vpin_vol_interaction'] = features['vpin'] * features['volatility']
            features['vpin_vol_ratio'] = features['vpin'] / (features['volatility'] + 1e-10)
        
        # 7. Fill NaN values (forward fill, then backward fill, then 0)
        # Use new pandas syntax instead of deprecated method parameter
        features = features.ffill().bfill().fillna(0)
        
        # Restore only numeric metadata columns if needed (regime for labeling)
        for col in ['regime']:  # Only restore numeric metadata columns
            if col in stored_metadata and col not in features.columns:
                features[col] = stored_metadata[col]
        
        # Store feature names (exclude target, metadata, and non-numeric columns)
        exclude_cols = ['timestamp', 'regime', 'regime_label', 'regime_confidence', 'ticker', 'event_type']
        
        # Only include numeric columns as features
        numeric_cols = features.select_dtypes(include=[np.number]).columns.tolist()
        
        # Double-check: exclude any string columns that might have snuck in
        for col in list(features.columns):
            if col not in exclude_cols and col in features.columns:
                # Check if column is actually numeric
                try:
                    is_numeric = pd.api.types.is_numeric_dtype(features[col]) if _PD_HAS_API_TYPES else pd.api.types.is_numeric_dtype(features[col])
                except:
                    # Fallback: try to convert to numeric and check if it fails
                    try:
                        pd.to_numeric(features[col], errors='raise')
                        is_numeric = True
                    except:
                        is_numeric = False
                
                if not is_numeric:
                    if col in numeric_cols:
                        numeric_cols.remove(col)
                    logger.warning(f"Excluding non-numeric column from features: {col}")
        
        self.feature_names = [col for col in numeric_cols if col not in exclude_cols]
        
        logger.info(f"Engineered {len(self.feature_names)} features")
        logger.debug(f"Feature names: {self.feature_names[:10]}...")  # Log first 10 features
        
        return features
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names (VPIN->vpin, vol->volatility, etc.)"""
        df = df.copy()
        column_mapping = {
            'VPIN': 'vpin',
            'vol': 'volatility',
            'Vol': 'volatility',
            'Volatility': 'volatility'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
        
        return df
    
    def _merge_sentiment_features(
        self, 
        features: pd.DataFrame, 
        sentiment_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge and create sentiment features."""
        if 'timestamp' not in features.columns or 'timestamp' not in sentiment_df.columns:
            logger.warning("Cannot merge sentiment: missing timestamp columns")
            return features
        
        sentiment_df = sentiment_df.copy()
        sentiment_df['timestamp'] = pd.to_datetime(sentiment_df['timestamp'], errors='coerce', utc=True)
        
        # Normalize sentiment column name
        sentiment_col = None
        for col in ['sentiment', 'sentiment_score', 'score']:
            if col in sentiment_df.columns:
                sentiment_col = col
                break
        
        if sentiment_col is None:
            logger.warning("No sentiment column found in sentiment_df")
            return features
        
        # Merge on timestamp (nearest)
        merged = pd.merge_asof(
            features.sort_values('timestamp'),
            sentiment_df[['timestamp', sentiment_col]].sort_values('timestamp'),
            on='timestamp',
            direction='nearest',
            tolerance=pd.Timedelta('1h')
        )
        
        if sentiment_col + '_y' in merged.columns:
            merged['sentiment'] = merged[sentiment_col + '_y']
            merged = merged.drop(columns=[sentiment_col + '_y'], errors='ignore')
        elif sentiment_col in merged.columns:
            merged['sentiment'] = merged[sentiment_col]
        
        # Create sentiment features
        if 'sentiment' in merged.columns:
            merged['sentiment_ma5'] = merged['sentiment'].rolling(5, min_periods=1).mean()
            merged['sentiment_ma20'] = merged['sentiment'].rolling(20, min_periods=1).mean()
            merged['sentiment_momentum'] = merged['sentiment'].diff(5)
            merged['sentiment_lag1'] = merged['sentiment'].shift(1)
            merged['sentiment_lag2'] = merged['sentiment'].shift(2)
            logger.debug("Added sentiment features")
        
        return merged
    
    def get_feature_names(self) -> List[str]:
        """Get list of engineered feature names."""
        return self.feature_names.copy()
    
    def get_feature_matrix(self, df: pd.DataFrame) -> np.ndarray:
        """Extract feature matrix from DataFrame - only numeric columns."""
        # Always exclude these columns no matter what
        always_exclude = ['event_type', 'ticker', 'regime_label', 'timestamp']
        
        # Get requested features that exist in DataFrame
        feature_cols = [col for col in self.feature_names if col in df.columns]
        
        # Remove any excluded columns
        feature_cols = [col for col in feature_cols if col not in always_exclude]
        
        if not feature_cols:
            available = [c for c in df.columns if c not in always_exclude]
            raise ValueError(
                f"No features found after filtering. "
                f"Requested: {self.feature_names[:10]}..., "
                f"Available (non-excluded): {available[:20]}..."
            )
        
        # Get ONLY numeric columns from DataFrame
        numeric_df = df.select_dtypes(include=[np.number])
        numeric_cols = numeric_df.columns.tolist()
        
        # Final filter: only use features that are numeric
        valid_feature_cols = [col for col in feature_cols if col in numeric_cols]
        
        if not valid_feature_cols:
            raise ValueError(
                f"No numeric features found. "
                f"Requested: {feature_cols[:10]}..., "
                f"Numeric columns in DF: {numeric_cols[:20]}..."
            )
        
        if len(valid_feature_cols) != len(feature_cols):
            excluded = set(feature_cols) - set(valid_feature_cols)
            logger.warning(f"{len(excluded)} non-numeric features excluded: {list(excluded)[:5]}...")
        
        # Extract data and convert to numeric array
        X = numeric_df[valid_feature_cols].values.copy()
        
        # Force to float64
        X = X.astype(np.float64, copy=False)
        
        # Replace infinite and NaN values
        X = np.nan_to_num(X, nan=0.0, posinf=1.0, neginf=-1.0)
        
        logger.debug(f"Extracted {len(valid_feature_cols)} features, matrix shape: {X.shape}")
        
        return X

