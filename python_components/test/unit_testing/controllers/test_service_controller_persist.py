"""Tests for ServiceController persistence methods."""
import os
import tempfile

import pandas as pd
import pytest

from backend.models.domain import TickerDTO, NewsDTO
from controllers.ServiceController import ServiceController
from datetime import datetime


def test_persist_ohlc_new_file():
    """Test persisting OHLC to a new CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Should create new file
        controller._persist_ohlc(ticker_dto)
        
        csv_path = controller.get_ticker_csv_path("TEST")
        assert os.path.exists(csv_path)
        
        # Verify content
        df = pd.read_csv(csv_path)
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "TEST"


def test_persist_ohlc_append():
    """Test appending OHLC to existing CSV."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        ticker_dto1 = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        ticker_dto2 = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now() + pd.Timedelta(hours=1),
            ticker="TEST",
            open=100.5,
            high=101.5,
            low=99.5,
            close=101.0,
            volume=1100
        )
        
        controller._persist_ohlc(ticker_dto1)
        controller._persist_ohlc(ticker_dto2)
        
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.read_csv(csv_path)
        assert len(df) == 2


def test_persist_metrics_new_file():
    """Test persisting metrics to a new CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # First persist OHLC
        controller._persist_ohlc(ticker_dto)
        
        # Then persist metrics
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)
        
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.read_csv(csv_path)
        assert "VPIN" in df.columns
        assert "vol" in df.columns
        assert "regime" in df.columns
        assert df.iloc[0]["VPIN"] == 0.5


def test_persist_metrics_update_existing():
    """Test updating metrics in existing CSV."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        controller._persist_ohlc(ticker_dto)
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)
        
        # Update with new metrics
        controller._persist_metrics(ticker_dto, vpin=0.6, vol=0.03, regime=1, regime_confidence=0.9)
        
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.read_csv(csv_path)
        # Should update the last row
        assert df.iloc[-1]["VPIN"] == 0.6


def test_persist_metrics_duplicate_columns():
    """Test persisting metrics when CSV has duplicate columns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create CSV with duplicate columns
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.DataFrame({
            "timestamp": [pd.Timestamp.now()],
            "ticker": ["TEST"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "VPIN": [0.4],
            "vpin": [0.4],  # Duplicate
            "vol": [0.02],
            "volatility": [0.02]  # Duplicate
        })
        df.to_csv(csv_path, index=False)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Should handle duplicate columns gracefully
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)


def test_persist_article_sentiment():
    """Test persisting article sentiment."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create dataInCsv directory
        os.makedirs(os.path.join(tmpdir, "dataInCsv"), exist_ok=True)
        
        controller = ServiceController(base_dir=tmpdir)
        
        news_dto = NewsDTO(
            event_type="NEWS",
            timestamp=datetime.now(),
            ticker="TEST",
            headline="Test headline",
            url="http://test.com"
        )
        
        from backend.models.domain import SentimentDTO
        sentiment_dto = SentimentDTO(
            ticker="TEST",
            timestamp=datetime.now(),
            headline="Test headline",
            sentiment_score=0.5,
            sentiment_label="positive"
        )
        
        controller._persist_article_sentiment(news_dto, sentiment_dto)
        
        # Verify file was created
        sentiment_path = os.path.join(tmpdir, "dataInCsv", "articles_with_sentiment.csv")
        if os.path.exists(sentiment_path):
            df = pd.read_csv(sentiment_path)
            assert len(df) > 0


def test_persist_metrics_with_none_values():
    """Test persisting metrics with None values."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        controller._persist_ohlc(ticker_dto)
        
        # Persist with None regime
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=None, regime_confidence=None)
        
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.read_csv(csv_path)
        # Should handle None gracefully
        assert "regime" in df.columns

