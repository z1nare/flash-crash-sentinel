"""Tests for ServiceController _persist_metrics method edge cases."""
import os
import tempfile

import pandas as pd
import pytest

from backend.models.domain import TickerDTO
from controllers.ServiceController import ServiceController


def test_persist_metrics_duplicate_column_handling():
    """Test _persist_metrics with duplicate vpin/volatility columns."""
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
        
        # Verify duplicate columns were removed
        df_after = pd.read_csv(csv_path)
        assert "vpin" not in df_after.columns or "VPIN" in df_after.columns
        assert "volatility" not in df_after.columns or "vol" in df_after.columns


def test_persist_metrics_timezone_handling():
    """Test _persist_metrics with timezone-aware timestamps."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create CSV with timezone-aware timestamp
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.DataFrame({
            "timestamp": [pd.Timestamp.now(tz="UTC")],
            "ticker": ["TEST"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000]
        })
        df.to_csv(csv_path, index=False)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now(tz="UTC"),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Should handle timezone normalization
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)


def test_persist_metrics_update_existing_row():
    """Test _persist_metrics updating existing row by timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create CSV with existing data
        csv_path = controller.get_ticker_csv_path("TEST")
        timestamp = pd.Timestamp.now()
        df = pd.DataFrame({
            "timestamp": [timestamp],
            "ticker": ["TEST"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000]
        })
        df.to_csv(csv_path, index=False)
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=timestamp,  # Same timestamp
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Should update existing row
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)
        
        df_after = pd.read_csv(csv_path)
        assert len(df_after) == 1  # Should still be 1 row (updated, not appended)
        assert df_after.iloc[0]["VPIN"] == 0.5


def test_persist_metrics_append_new_row():
    """Test _persist_metrics appending new row when timestamp doesn't match."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create CSV with existing data
        csv_path = controller.get_ticker_csv_path("TEST")
        df = pd.DataFrame({
            "timestamp": [pd.Timestamp.now()],
            "ticker": ["TEST"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000]
        })
        df.to_csv(csv_path, index=False)
        
        # Different timestamp
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp.now() + pd.Timedelta(hours=1),
            ticker="TEST",
            open=100.5,
            high=101.5,
            low=99.5,
            close=101.0,
            volume=1100
        )
        
        # Should append new row
        controller._persist_metrics(ticker_dto, vpin=0.6, vol=0.03, regime=1, regime_confidence=0.9)
        
        df_after = pd.read_csv(csv_path)
        assert len(df_after) == 2  # Should have 2 rows


def test_persist_metrics_old_pandas_versions():
    """Test _persist_metrics with CSV that triggers old pandas error handling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        controller = ServiceController(base_dir=tmpdir)
        
        # Create malformed CSV
        csv_path = controller.get_ticker_csv_path("TEST")
        with open(csv_path, "w") as f:
            f.write("timestamp,ticker,open,high,low,close,volume\n")
            f.write("invalid,data,here\n")  # Bad row
            f.write("2024-01-01 09:30:00,TEST,100,101,99,100.5,1000\n")
        
        ticker_dto = TickerDTO(
            event_type="TICK",
            timestamp=pd.Timestamp("2024-01-01 09:30:00"),
            ticker="TEST",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        # Should handle malformed CSV gracefully
        controller._persist_metrics(ticker_dto, vpin=0.5, vol=0.02, regime=0, regime_confidence=0.8)

