"""
Background Data Replay Service for Streamlit Dashboard

Runs the Bloomberg data replay simulator in the background
when auto-refresh is enabled in the dashboard.
"""
import pandas as pd
import requests
import time
import threading
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path

# Configuration
# BloombergData can be in parent directory (local) or /app/BloombergData (Docker)
_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_bb_dir_parent = os.path.join(_base_dir, "..", "BloombergData")
_bb_dir_docker = "/app/BloombergData"

if os.path.exists(_bb_dir_docker):
    BLOOMBERG_DATA_DIR = _bb_dir_docker  # Docker mount
elif os.path.exists(_bb_dir_parent):
    BLOOMBERG_DATA_DIR = os.path.abspath(_bb_dir_parent)  # Parent directory (local)
else:
    BLOOMBERG_DATA_DIR = os.path.join(_base_dir, "BloombergData")  # Fallback

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

class DataReplayService:
    """Background service for replaying Bloomberg Excel data."""
    
    def __init__(self, api_url: str = API_BASE_URL, speed: float = 1.0):
        """
        Initialize the replay service.
        
        Args:
            api_url: Base URL of the FastAPI backend
            speed: Playback speed multiplier (1.0 = real-time)
        """
        self.api_url = api_url
        self.speed = speed
        self.interval_seconds = 10.0 / speed
        self.running = False
        self.thread = None
        self.current_row_index = {}  # Track progress per ticker
        self.tickers_data = {}  # Cache loaded data per ticker
        self.last_send_time = {}  # Track last send time per ticker
    
    def load_excel_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load Excel file for a ticker."""
        if ticker in self.tickers_data:
            return self.tickers_data[ticker]
        
        excel_file = os.path.join(BLOOMBERG_DATA_DIR, f"{ticker.upper()}10sec.xlsx")
        
        if not os.path.exists(excel_file):
            return None
        
        try:
            df = pd.read_excel(excel_file, engine='openpyxl')
            df.columns = df.columns.str.strip().str.lower()
            
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return None
            
            for col in required_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna(subset=required_cols)
            df = df.reset_index(drop=True)
            
            # Cache the data
            self.tickers_data[ticker] = df
            return df
            
        except Exception as e:
            print(f"Error loading {excel_file}: {e}")
            return None
    
    def send_tick_to_api(self, ticker: str, row: pd.Series, timestamp: datetime) -> bool:
        """Send a single tick to the API."""
        try:
            payload = {
                "event_type": "TICK",
                "timestamp": timestamp.isoformat(),
                "ticker": ticker.upper(),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": int(row['volume'])
            }
            
            response = requests.post(
                f"{self.api_url}/api/tick",
                json=payload,
                timeout=5
            )
            
            return response.status_code == 200
                
        except Exception:
            return False
    
    def _replay_loop(self, tickers: List[str]):
        """Background thread loop that sends ticks periodically."""
        # Initialize progress tracking
        for ticker in tickers:
            ticker_upper = ticker.upper()
            if ticker_upper not in self.current_row_index:
                self.current_row_index[ticker_upper] = 0
            if ticker_upper not in self.last_send_time:
                self.last_send_time[ticker_upper] = datetime.now() - timedelta(seconds=self.interval_seconds)
        
        # Load all ticker data upfront
        ticker_data = {}
        for ticker in tickers:
            ticker_upper = ticker.upper()
            df = self.load_excel_data(ticker_upper)
            if df is not None and not df.empty:
                ticker_data[ticker_upper] = df
        
        if not ticker_data:
            print("No valid ticker data found for replay")
            self.running = False
            return
        
        print(f"Data replay started for tickers: {list(ticker_data.keys())}")
        
        while self.running:
            current_time = datetime.now()
            
            for ticker_upper in ticker_data.keys():
                # Check if enough time has passed for this ticker
                if ticker_upper not in self.last_send_time:
                    self.last_send_time[ticker_upper] = current_time - timedelta(seconds=self.interval_seconds)
                
                time_since_last = (current_time - self.last_send_time[ticker_upper]).total_seconds()
                
                if time_since_last >= self.interval_seconds:
                    df = ticker_data[ticker_upper]
                    
                    # Get current row index
                    if ticker_upper not in self.current_row_index:
                        self.current_row_index[ticker_upper] = 0
                    
                    row_idx = self.current_row_index[ticker_upper] % len(df)
                    row = df.iloc[row_idx]
                    
                    # Send tick to API
                    success = self.send_tick_to_api(ticker_upper, row, current_time)
                    
                    if success:
                        # Update tracking only on success
                        self.current_row_index[ticker_upper] = (row_idx + 1) % len(df)  # Loop around
                        self.last_send_time[ticker_upper] = current_time
            
            # Sleep for a short interval before checking again
            time.sleep(min(0.5, self.interval_seconds / 4))
    
    def start(self, tickers: List[str]):
        """Start replaying data for the given tickers."""
        if self.running:
            return
        
        # Validate tickers have data
        valid_tickers = []
        for ticker in tickers:
            df = self.load_excel_data(ticker)
            if df is not None and not df.empty:
                valid_tickers.append(ticker.upper())
        
        if not valid_tickers:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._replay_loop, args=(valid_tickers,), daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop the replay service."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
    
    def is_running(self) -> bool:
        """Check if the service is currently running."""
        return self.running and (self.thread is not None and self.thread.is_alive())

# Global service instance (singleton pattern)
_service_instance: Optional[DataReplayService] = None

def get_replay_service() -> DataReplayService:
    """Get or create the global replay service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = DataReplayService()
    return _service_instance

def start_replay(tickers: List[str]) -> bool:
    """Start replaying data for tickers."""
    service = get_replay_service()
    service.start(tickers)
    return service.is_running()

def stop_replay():
    """Stop the replay service."""
    service = get_replay_service()
    service.stop()

def is_replay_running() -> bool:
    """Check if replay is currently running."""
    service = get_replay_service()
    return service.is_running()

