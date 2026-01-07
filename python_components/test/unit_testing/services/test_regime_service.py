from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from services.regime_service import RegimeDetectionService, SklearnRegimeModel


class _NoProbaModel:
    """Minimal model with predict() but no predict_proba()."""

    def fit(self, X, y):
        return self

    def predict(self, X):
        # cycle 0,1,2,...
        n = len(X)
        return np.array([i % 3 for i in range(n)], dtype=int)


def test_sklearn_regime_model_predict_proba_falls_back_to_one_hot_when_missing():
    wrapper = SklearnRegimeModel(model=_NoProbaModel(), scaler=None)
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04], [0.2, 0.01]])
    probs = wrapper.predict_proba(X)
    assert probs.shape[0] == len(X)
    assert probs.shape[1] == 3
    assert np.allclose(probs.sum(axis=1), 1.0)


def test_regime_detection_service_predict_regime_returns_default_for_invalid_inputs():
    svc = RegimeDetectionService(model=SklearnRegimeModel(model=_NoProbaModel()))
    state, conf = svc.predict_regime(vpin=float("nan"), volatility=0.01)
    assert state == 0
    assert conf == 0.0


def test_regime_detection_service_predict_regimes_batch_accepts_alt_column_names():
    svc = RegimeDetectionService(model=SklearnRegimeModel(model=_NoProbaModel()))
    df = pd.DataFrame(
        {
            "VPIN": [0.2, 0.3, np.nan],
            "vol": [0.01, 0.02, 0.03],
        }
    )
    out = svc.predict_regimes_batch(df)
    assert "regime" in out.columns
    assert "regime_confidence" in out.columns
    # first two rows are valid, third invalid
    assert out["regime"].iloc[0] in (0, 1, 2)
    assert out["regime"].iloc[1] in (0, 1, 2)
    assert out["regime"].iloc[2] is None


def test_get_regime_label_maps_known_states():
    svc = RegimeDetectionService(model=SklearnRegimeModel(model=_NoProbaModel()))
    assert "Normal" in svc.get_regime_label(0)
    assert "Correction" in svc.get_regime_label(1)
    assert "Crash" in svc.get_regime_label(2)


def test_sklearn_regime_model_save_and_load_roundtrip(tmp_path):
    wrapper = SklearnRegimeModel(model=_NoProbaModel(), scaler=None)
    p = tmp_path / "model.pkl"
    wrapper.save(str(p))
    loaded = SklearnRegimeModel.load(str(p))
    X = np.array([[0.1, 0.02], [0.5, 0.03], [0.9, 0.04]])
    assert np.allclose(loaded.predict_proba(X).sum(axis=1), 1.0)


