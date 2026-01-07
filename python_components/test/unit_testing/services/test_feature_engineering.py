from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from services.feature_engineering import FeatureEngineer


def _market_df(rows: int = 30) -> pd.DataFrame:
    ts0 = datetime(2026, 1, 1, 9, 30)
    out = []
    px = 100.0
    for i in range(rows):
        ts = ts0 + timedelta(minutes=i)
        px = px * (1.0 + (0.001 if i % 2 == 0 else -0.0007))
        out.append(
            {
                "event_type": "TICK",
                "timestamp": ts.isoformat(),
                "ticker": "TEST",
                "open": px,
                "high": px * 1.001,
                "low": px * 0.999,
                "close": px * 1.0002,
                "volume": 1000 + i,
                "VPIN": 0.2 + (i % 5) * 0.05,
                "vol": 0.01 + (i % 3) * 0.001,
            }
        )
    return pd.DataFrame(out)


def test_normalize_columns_maps_vpin_and_volatility():
    fe = FeatureEngineer()
    df = pd.DataFrame({"VPIN": [0.1], "vol": [0.02]})
    out = fe._normalize_columns(df)
    assert "vpin" in out.columns
    assert "volatility" in out.columns
    assert float(out["vpin"].iloc[0]) == pytest.approx(0.1)
    assert float(out["volatility"].iloc[0]) == pytest.approx(0.02)


def test_engineer_features_adds_expected_feature_columns_and_sets_feature_names():
    fe = FeatureEngineer()
    features = fe.engineer_features(_market_df())
    # A few representative engineered columns
    assert "vpin_ma5" in features.columns
    assert "vol_ma5" in features.columns
    assert "price_momentum" in features.columns
    assert "volume_spike" in features.columns
    assert len(fe.get_feature_names()) > 0


def test_engineer_features_preserves_regime_numeric_column_if_present():
    fe = FeatureEngineer()
    df = _market_df()
    df["regime"] = 1
    out = fe.engineer_features(df)
    assert "regime" in out.columns
    assert int(out["regime"].iloc[-1]) == 1


def test_merge_sentiment_features_noop_when_missing_timestamp_column():
    fe = FeatureEngineer()
    base = fe.engineer_features(_market_df())
    sent = pd.DataFrame({"sentiment_score": [0.5]})
    out = fe._merge_sentiment_features(base.copy(), sent)
    # should not crash or add sentiment columns without timestamps
    assert "sentiment" not in out.columns


def test_get_feature_matrix_raises_when_no_features_available():
    fe = FeatureEngineer()
    fe.feature_names = ["does_not_exist"]
    with pytest.raises(ValueError):
        fe.get_feature_matrix(pd.DataFrame({"timestamp": [datetime(2026, 1, 1)]}))


def test_get_feature_matrix_returns_numeric_matrix_and_is_finite():
    fe = FeatureEngineer()
    df = fe.engineer_features(_market_df())
    X = fe.get_feature_matrix(df)
    assert isinstance(X, np.ndarray)
    assert X.ndim == 2
    assert X.shape[0] == len(df)
    assert X.shape[1] == len([c for c in fe.get_feature_names() if c in df.columns])
    assert np.isfinite(X).all()


