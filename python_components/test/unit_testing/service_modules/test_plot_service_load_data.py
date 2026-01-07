from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

import services.plotService as plot_mod


def test_load_data_returns_empty_when_market_csv_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(plot_mod, "HISTORICAL_DATA_DIR", str(tmp_path / "historicalData"))
    (tmp_path / "historicalData").mkdir(parents=True, exist_ok=True)

    df, ticker = plot_mod.load_data(ticker="MISSING", sentiment_path=str(tmp_path / "sent.csv"))
    assert df.empty
    assert ticker is None


def test_load_data_deduplicates_case_collisions_and_produces_numeric_columns(tmp_path, monkeypatch):
    hist = tmp_path / "historicalData"
    hist.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(plot_mod, "HISTORICAL_DATA_DIR", str(hist))

    # Create a CSV with duplicate columns after lowercasing: VPIN vs vpin, vol vs volatility
    df_in = pd.DataFrame(
        {
            "timestamp": [datetime(2026, 1, 1, 9, 30).isoformat()],
            "ticker": ["AMD"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "VPIN": [0.8],
            "vpin": [0.2],  # should win (keep='last')
            "vol": [0.01],
            "volatility": [0.02],  # should win (keep='last')
        }
    )
    df_in.to_csv(hist / "AMD.csv", index=False)

    df, ticker = plot_mod.load_data(ticker="AMD", sentiment_path=str(tmp_path / "missing_sent.csv"))
    assert ticker == "AMD"
    assert not df.empty
    assert "vpin" in df.columns
    assert "volatility" in df.columns
    # last-occurrence should be kept
    assert float(df["vpin"].iloc[-1]) == pytest.approx(0.2)
    assert float(df["volatility"].iloc[-1]) == pytest.approx(0.02)


def test_load_data_drops_invalid_numeric_timestamps(tmp_path, monkeypatch):
    hist = tmp_path / "historicalData"
    hist.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(plot_mod, "HISTORICAL_DATA_DIR", str(hist))

    df_in = pd.DataFrame(
        {
            "timestamp": [123.0],  # invalid (too small to be unix epoch)
            "ticker": ["SPY"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    df_in.to_csv(hist / "SPY.csv", index=False)

    df, ticker = plot_mod.load_data(ticker="SPY", sentiment_path=str(tmp_path / "missing_sent.csv"))
    assert df.empty
    assert ticker is None


def test_load_data_merges_sentiment_with_defaults_when_missing(tmp_path, monkeypatch):
    hist = tmp_path / "historicalData"
    data_in = tmp_path / "dataInCsv"
    hist.mkdir(parents=True, exist_ok=True)
    data_in.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(plot_mod, "HISTORICAL_DATA_DIR", str(hist))

    pd.DataFrame(
        {
            "timestamp": [datetime(2026, 1, 1, 9, 30).isoformat()],
            "ticker": ["NVDA"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "vpin": [0.5],
            "vol": [0.01],
        }
    ).to_csv(hist / "NVDA.csv", index=False)

    # sentiment file exists but has no NVDA rows -> should default to 0 sentiment + empty headline
    (data_in / "articles_with_sentiment.csv").write_text(
        "timestamp,ticker,headline,sentiment_score,sentiment_label,url\n"
        "2026-01-01T09:00:00,AAPL,Example,0.5,positive,https://example.com\n",
        encoding="utf-8",
    )

    df, ticker = plot_mod.load_data(ticker="NVDA", sentiment_path=str(data_in / "articles_with_sentiment.csv"))
    assert ticker == "NVDA"
    assert "sentiment_score" in df.columns
    assert "headline" in df.columns
    assert float(df["sentiment_score"].iloc[-1]) == pytest.approx(0.0)
    assert str(df["headline"].iloc[-1]) == ""


def test_load_data_recomputes_volatility_when_all_zero(tmp_path, monkeypatch):
    hist = tmp_path / "historicalData"
    hist.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(plot_mod, "HISTORICAL_DATA_DIR", str(hist))

    df_in = pd.DataFrame(
        {
            "timestamp": [
                datetime(2026, 1, 1, 9, 30).isoformat(),
                datetime(2026, 1, 2, 9, 30).isoformat(),
            ],
            "ticker": ["TEST", "TEST"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1000],
            "vpin": [0.5, 0.5],
            "volatility": [0.0, 0.0],  # triggers recompute
        }
    )
    df_in.to_csv(hist / "TEST.csv", index=False)

    # Monkeypatch the rolling volatility calculator to a known output
    monkeypatch.setattr(
        plot_mod.VolatilityService,
        "calculate_rolling_volatility",
        staticmethod(lambda df: [0.123] * len(df)),
    )

    df, ticker = plot_mod.load_data(ticker="TEST", sentiment_path=str(tmp_path / "missing_sent.csv"))
    assert ticker == "TEST"
    assert float(df["volatility"].iloc[-1]) == pytest.approx(0.123)


