from __future__ import annotations

import math
import pickle
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from services.hmm_regime_service import HMMRegimeService


@dataclass
class DummyHMMModel:
    """
    Minimal stand-in for hmmlearn's GaussianHMM used to unit-test our glue logic
    without depending on hmmlearn or doing expensive training.
    """

    n_components: int = 3

    def predict(self, X):
        X = np.asarray(X)
        # Simple deterministic state: bucket by vpin level
        vpin = X[:, 0]
        return np.where(vpin < 0.33, 0, np.where(vpin < 0.66, 1, 2))

    def score_samples(self, X):
        X = np.asarray(X)
        vpin = X[:, 0]
        # Construct pseudo-probabilities, then return log-probs (the service code exp()s them)
        p0 = np.clip(1.0 - vpin, 1e-6, 1.0)
        p2 = np.clip(vpin, 1e-6, 1.0)
        p1 = np.clip(1.0 - np.abs(vpin - 0.5) * 2.0, 1e-6, 1.0)
        probs = np.stack([p0, p1, p2], axis=1)
        probs = probs / probs.sum(axis=1, keepdims=True)
        log_probs = np.log(probs)
        return 0.0, log_probs


def test_predict_regime_raises_when_model_missing():
    svc = HMMRegimeService()
    with pytest.raises(ValueError):
        svc.predict_regime(vpin=0.1, volatility=0.01)


def test_predict_regime_invalid_input_returns_default():
    svc = HMMRegimeService()
    svc.model = DummyHMMModel()
    state, conf = svc.predict_regime(vpin=float("nan"), volatility=0.01)
    assert state == 0
    assert conf == 0.0


def test_predict_regime_returns_state_and_confidence_in_range():
    svc = HMMRegimeService()
    svc.model = DummyHMMModel()
    state, conf = svc.predict_regime(vpin=0.8, volatility=0.02)
    assert state in {0, 1, 2}
    assert 0.0 <= conf <= 1.0


def test_predict_regimes_batch_accepts_VPIN_vol_columns_and_filters_invalid_rows():
    svc = HMMRegimeService()
    svc.model = DummyHMMModel()

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=4, freq="min"),
            "VPIN": [0.1, 0.5, np.inf, 0.9],
            "vol": [0.01, 0.02, 0.03, np.nan],
        }
    )

    out = svc.predict_regimes_batch(df)
    assert "regime" in out.columns
    assert "regime_confidence" in out.columns

    # Row with inf should be invalid -> NaN regime
    assert math.isnan(float(out.loc[2, "regime"]))
    # Row with NaN volatility should be invalid -> NaN regime
    assert math.isnan(float(out.loc[3, "regime"]))
    # Valid rows should have numeric regime + confidence
    assert float(out.loc[0, "regime"]) in {0.0, 1.0, 2.0}
    assert 0.0 <= float(out.loc[0, "regime_confidence"]) <= 1.0


def test_save_model_raises_when_model_missing(tmp_path):
    svc = HMMRegimeService()
    with pytest.raises(ValueError):
        svc.save_model(str(tmp_path / "model.pkl"))


def test_save_model_writes_pickle_to_disk(tmp_path):
    svc = HMMRegimeService()
    svc.model = DummyHMMModel()

    out_path = tmp_path / "nested" / "model.pkl"
    svc.save_model(str(out_path))
    assert out_path.exists()

    loaded = pickle.loads(out_path.read_bytes())
    assert isinstance(loaded, DummyHMMModel)


def test_train_from_csv_preprocesses_and_calls_train_model(tmp_path, monkeypatch):
    # Build 110 rows so it passes the >=100 requirement.
    rows = []
    for i in range(110):
        rows.append(
            {
                "timestamp": f"2026-01-01T09:{i%60:02d}:00+00:00",
                "ticker": "TeSt",
                "VPIN": 0.2 + (i % 5) * 0.01,
                "vol": 0.01 + (i % 7) * 0.001,
            }
        )
    csv_path = tmp_path / "hist.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    called = {"n": 0, "len": None}

    def fake_train_model(self, data, n_states=3, n_iter=100, random_state=42):
        called["n"] += 1
        called["len"] = len(data)
        self.model = DummyHMMModel(n_components=n_states)

    monkeypatch.setattr(HMMRegimeService, "train_model", fake_train_model, raising=True)

    svc = HMMRegimeService.train_from_csv(str(csv_path), ticker="TEST", output_model_path=None, n_states=3)
    assert called["n"] == 1
    assert called["len"] is not None and called["len"] >= 100
    assert svc.model is not None


