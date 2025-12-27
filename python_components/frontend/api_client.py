"""
FastAPI Client for Streamlit Dashboard
Handles all API communication with the FastAPI backend
"""
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
import os

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

class APIClient:
    """Client for communicating with FastAPI backend"""
    
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url
        self.timeout = 30
    
    def check_connection(self) -> bool:
        """Check if API is reachable"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of all services"""
        try:
            response = requests.get(f"{self.base_url}/api/status", timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def list_tickers(self) -> List[str]:
        """Get list of available tickers"""
        try:
            response = requests.get(f"{self.base_url}/api/tickers", timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("tickers", [])
        except Exception as e:
            return []
    
    def get_metrics_history(self, ticker: str, limit: int = 1000) -> pd.DataFrame:
        """Get historical metrics for a ticker"""
        try:
            response = requests.get(
                f"{self.base_url}/api/metrics/history",
                params={"ticker": ticker, "limit": limit},
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
                df['timestamp'] = df['timestamp'].dt.tz_convert(None)
            
            # Normalize column names (handle both 'vpin' and 'VPIN', 'volatility' and 'vol')
            if 'VPIN' in df.columns and 'vpin' not in df.columns:
                df['vpin'] = df['VPIN']
            elif 'vpin' in df.columns and 'VPIN' not in df.columns:
                df['VPIN'] = df['vpin']
            
            if 'vol' in df.columns and 'volatility' not in df.columns:
                df['volatility'] = df['vol']
            elif 'volatility' in df.columns and 'vol' not in df.columns:
                df['vol'] = df['volatility']
            
            return df
        except Exception as e:
            return pd.DataFrame()
    
    def get_latest_metrics(self, ticker: str) -> Dict[str, Any]:
        """Get latest metrics for a ticker"""
        df = self.get_metrics_history(ticker, limit=1)
        if df.empty:
            return {}
        
        latest = df.iloc[0].to_dict()
        return latest
    
    def process_tick(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a new tick"""
        try:
            response = requests.post(
                f"{self.base_url}/api/tick",
                json=tick_data,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def process_news(self, news_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process news article"""
        try:
            response = requests.post(
                f"{self.base_url}/api/news",
                json=news_data,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of text"""
        try:
            response = requests.post(
                f"{self.base_url}/api/sentiment/analyze",
                json={"text": text},
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def get_market_data(self, ticker: str) -> pd.DataFrame:
        """Get full market data (OHLC + metrics) for a ticker"""
        # Read directly from CSV file (same data API uses)
        import os
        # Get python_components directory (parent of frontend)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "historicalData", f"{ticker.upper()}.csv")
        
        if not os.path.exists(csv_path):
            # Try alternative location
            alt_path = os.path.join(base_dir, "dataInCsv", "data.csv")
            if os.path.exists(alt_path):
                csv_path = alt_path
            else:
                return pd.DataFrame()
        
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            
            if df.empty:
                return pd.DataFrame()
            
            # Filter by ticker if ticker column exists (but file is already ticker-specific)
            if 'ticker' in df.columns:
                df = df[df['ticker'].str.upper() == ticker.upper()]
            
            if df.empty:
                return pd.DataFrame()
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
                df['timestamp'] = df['timestamp'].dt.tz_convert(None)
                df = df.dropna(subset=['timestamp'])
            
            # Ensure we have OHLC data
            required_cols = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                return pd.DataFrame()
            
            # Ensure VPIN and vol columns are present (might be uppercase/lowercase)
            if 'VPIN' not in df.columns and 'vpin' in df.columns:
                df['VPIN'] = df['vpin']
            if 'vol' not in df.columns and 'volatility' in df.columns:
                df['vol'] = df['volatility']
            
            return df.sort_values('timestamp', ascending=True)
        except Exception as e:
            import traceback
            print(f"Error loading market data: {e}")
            print(traceback.format_exc())
            return pd.DataFrame()
    
    def get_sentiment_data(self) -> pd.DataFrame:
        """Get sentiment data from articles"""
        import os
        # Get python_components directory (parent of frontend)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "dataInCsv", "articles_with_sentiment.csv")
        
        if not os.path.exists(csv_path):
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            
            if df.empty:
                return pd.DataFrame()
            
            # Ensure required columns exist
            required_cols = ['headline', 'sentiment_score', 'sentiment_label']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                # Try alternative column names
                if 'sentiment' in df.columns:
                    # Might be a combined sentiment column, skip for now
                    pass
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
                df['timestamp'] = df['timestamp'].dt.tz_convert(None)
                df = df.dropna(subset=['timestamp'])
            
            return df.sort_values('timestamp', ascending=False)
        except Exception as e:
            return pd.DataFrame()
    
    def connect_ib(self) -> Dict[str, Any]:
        """Connect to Interactive Brokers"""
        try:
            response = requests.post(
                f"{self.base_url}/api/ib/connect",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def disconnect_ib(self) -> Dict[str, Any]:
        """Disconnect from Interactive Brokers"""
        try:
            response = requests.post(
                f"{self.base_url}/api/ib/disconnect",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"success": False, "error": str(e)}

