import os
import pandas as pd
from typing import Optional, Tuple
from pathlib import Path
from backend.models.domain import TickerDTO, NewsDTO
from services.vpin_service import VpinService
from services.vol_service import VolatilityService
from services.sentimentService import SentimentService
from services.regime_service import RegimeDetectionService

class ServiceController:
    def __init__(self):
        # Base directory for historical data
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.HISTORICAL_DATA_DIR = os.path.join(BASE_DIR, "historicalData")
        
        # Shared paths (not ticker-specific)
        self.ARTICLES_CSV_PATH = os.path.join(BASE_DIR, "dataInCsv", "articles.csv")
        
        # Ensure historicalData directory exists
        os.makedirs(self.HISTORICAL_DATA_DIR, exist_ok=True)

        # Initialize services (volatility service will use ticker-specific paths dynamically)
        self.vpin_service = VpinService()
        # VolatilityService will be initialized per-ticker with correct CSV path
        self.vol_services = {}  # Cache of VolatilityService instances per ticker
        self.sentiment_service = SentimentService()
        
        # Regime detection models directory (from experiments)
        self.EXPERIMENTS_MODELS_DIR = os.path.join(BASE_DIR, "experiments", "regime_detection", "models")
        # Fallback to old HMM directory for backwards compatibility
        self.LEGACY_MODELS_DIR = os.path.join(BASE_DIR, "models", "hmm")
        os.makedirs(self.EXPERIMENTS_MODELS_DIR, exist_ok=True)
        os.makedirs(self.LEGACY_MODELS_DIR, exist_ok=True)
        self.regime_services = {}  # Cache of RegimeDetectionService instances per ticker
        
        # Interactive Brokers service (optional)
        self.ib_service = None
        try:
            from services.ib_client_service import IBClientService
            from utils.logger import get_service_logger
            logger = get_service_logger("controller")
            self.ib_service = IBClientService(host="127.0.0.1", port=7497, client_id=1)
            # Auto-connect if environment variable is set
            if os.getenv("USE_IB_REALTIME", "false").lower() == "true":
                logger.info("Auto-connecting to IB (USE_IB_REALTIME=true)")
                self.ib_service.connect()
        except Exception as e:
            # IB not available or failed to initialize - continue without it
            pass
    
    def get_ticker_csv_path(self, ticker: str) -> str:
        """
        Get the CSV file path for a specific ticker in historicalData folder.
        
        Args:
            ticker: Stock ticker symbol (e.g., "NVDA", "TSLA")
        
        Returns:
            Full path to ticker-specific CSV file
        """
        return os.path.join(self.HISTORICAL_DATA_DIR, f"{ticker.upper()}.csv")
    
    def get_volatility_service(self, ticker: str) -> VolatilityService:
        """
        Get or create a VolatilityService instance for a specific ticker.
        Each ticker gets its own service instance pointing to its CSV file.
        
        Args:
            ticker: Stock ticker symbol
        
        Returns:
            VolatilityService instance configured for this ticker's CSV
        """
        if ticker not in self.vol_services:
            csv_path = self.get_ticker_csv_path(ticker)
            self.vol_services[ticker] = VolatilityService(csv_path=csv_path)
        return self.vol_services[ticker]
    
    def get_regime_service(self, ticker: str) -> Optional[RegimeDetectionService]:
        """
        Get or load regime detection model for a specific ticker.
        First tries to load from experiment results, then falls back to legacy HMM model.
        
        Args:
            ticker: Stock ticker symbol
        
        Returns:
            RegimeDetectionService instance if model exists, None otherwise
        """
        if ticker not in self.regime_services:
            # Try loading from experiment results first (best model from CV)
            experiment_model_path = os.path.join(self.EXPERIMENTS_MODELS_DIR, f"{ticker}_best_model.pkl")
            if os.path.exists(experiment_model_path):
                try:
                    self.regime_services[ticker] = RegimeDetectionService.load_from_experiment_results(
                        ticker=ticker,
                        experiments_dir=Path(self.EXPERIMENTS_MODELS_DIR).parent / "models"
                    )
                    print(f"[CONTROLLER] Loaded best regime model for {ticker} from experiments")
                except Exception as e:
                    print(f"[CONTROLLER] Failed to load experiment model for {ticker}: {e}")
                    # Fall through to legacy model
            else:
                # Try legacy HMM model path for backwards compatibility
                legacy_model_path = os.path.join(self.LEGACY_MODELS_DIR, f"{ticker}_hmm.pkl")
                if os.path.exists(legacy_model_path):
                    try:
                        from services.hmm_regime_service import HMMRegimeService
                        # Wrap legacy HMM in new interface
                        legacy_hmm = HMMRegimeService(model_path=legacy_model_path)
                        # Create a wrapper to make it compatible
                        from services.regime_service import SklearnRegimeModel
                        # We'll need to adapt this, but for now just load directly
                        print(f"[CONTROLLER] ⚠️  Using legacy HMM model for {ticker}. Consider running experiments to get best model.")
                        # For backwards compatibility, we'll still use the old service
                        # but mark it differently
                        return None  # Will use old method below
                    except Exception as e:
                        print(f"[CONTROLLER] Failed to load legacy model for {ticker}: {e}")
        
        return self.regime_services.get(ticker)
        
    def process_tick(self, ticker_dto: TickerDTO) -> None:
        """
        Orchestrates the flow:
        1. Receive new OHLC ticker data.
        2. Persist it to the ticker-specific CSV file in historicalData/.
        3. Calculate VPIN (updates bucket state).
        4. IF VPIN bucket is full (metrics ready):
            a. Calculate Volatility (using the updated history from ticker-specific CSV).
            b. Persist the combined metrics (VPIN + Vol) to the same ticker-specific CSV.
        """
        ticker = ticker_dto.ticker.upper()
        
        # 1. Persist Raw Tick to ticker-specific CSV
        self._persist_ohlc(ticker_dto)
        
        # 2. Calculate VPIN
        vpin_score = self.vpin_service.process_tick(ticker_dto)
        
        # 3. Check if we need to generate metrics
        if vpin_score is not None:
            try:
                # a. Calculate Volatility using ticker-specific service
                vol_service = self.get_volatility_service(ticker)
                vol_score = vol_service.process_tick(ticker_dto)
                
                # b. Predict regime using best model (if available)
                regime_state, regime_confidence = None, None
                regime_service = self.get_regime_service(ticker)
                if regime_service and vol_score > 0:
                    try:
                        regime_state, regime_confidence = regime_service.predict_regime(vpin_score, vol_score)
                        regime_label = regime_service.get_regime_label(regime_state)
                        print(f"[CONTROLLER] Regime: {regime_state} ({regime_label}), Confidence: {regime_confidence:.2%}")
                    except Exception as e:
                        print(f"[CONTROLLER] Error predicting regime for {ticker}: {e}")
                else:
                    # Try legacy HMM service for backwards compatibility
                    legacy_model_path = os.path.join(self.LEGACY_MODELS_DIR, f"{ticker}_hmm.pkl")
                    if os.path.exists(legacy_model_path) and vol_score > 0:
                        try:
                            from services.hmm_regime_service import HMMRegimeService
                            legacy_hmm = HMMRegimeService(model_path=legacy_model_path)
                            regime_state, regime_confidence = legacy_hmm.predict_regime(vpin_score, vol_score)
                            regime_label = legacy_hmm.get_regime_label(regime_state)
                            print(f"[CONTROLLER] Regime (legacy HMM): {regime_state} ({regime_label}), Confidence: {regime_confidence:.2%}")
                        except Exception as e:
                            print(f"[CONTROLLER] Error with legacy HMM for {ticker}: {e}")
                
                # c. Persist Metrics (VPIN + Volatility + Regime) to ticker-specific CSV
                self._persist_metrics(ticker_dto, vpin_score, vol_score, regime_state, regime_confidence)
                print(f"[CONTROLLER] Metric Saved: {ticker} | VPIN: {vpin_score:.4f} | Vol: {vol_score:.6f}" + 
                      (f" | Regime: {regime_state}" if regime_state is not None else ""))
            except Exception as e:
                print(f"[CONTROLLER] Error calculating volatility for {ticker}: {str(e)}")
                # Still persist VPIN even if volatility fails
                vol_score = 0.0
                self._persist_metrics(ticker_dto, vpin_score, vol_score, None, None)
                print(f"[CONTROLLER] Metric Saved (VPIN only): {ticker} | VPIN: {vpin_score:.4f} | Vol: 0.0 (error)")

    def process_news(self, news_dto: NewsDTO):
        """
        Orchestrates the News Flow:
        1. Receive NewsDTO (headline, timestamp, etc.)
        2. Call SentimentService to get score/label.
        3. Persist result to articles.csv.
        
        Returns:
            SentimentDTO: The calculated sentiment result
        """
        print(f"[CONTROLLER] Processing News: {news_dto.headline[:50]}...")
        
        # 1. Analyze Sentiment
        sentiment_dto = self.sentiment_service.process_news(news_dto)
        
        # 2. Persist
        self._persist_article_sentiment(news_dto, sentiment_dto)
        print(f" -> Sentiment Saved: {sentiment_dto.sentiment_label} ({sentiment_dto.sentiment_score:.4f})")
        
        return sentiment_dto

    def _persist_ohlc(self, ticker_dto: TickerDTO) -> None:
        """Append raw tick to ticker-specific CSV file in historicalData/"""
        ticker = ticker_dto.ticker.upper()
        csv_path = self.get_ticker_csv_path(ticker)
        
        new_row = {
            "event_type": ticker_dto.event_type,
            "timestamp": ticker_dto.timestamp,
            "ticker": ticker_dto.ticker,
            "open": ticker_dto.open,
            "high": ticker_dto.high,
            "low": ticker_dto.low,
            "close": ticker_dto.close,
            "volume": ticker_dto.volume
        }
        df_new = pd.DataFrame([new_row])
        
        # Append to ticker-specific CSV
        if not os.path.exists(csv_path):
            df_new.to_csv(csv_path, index=False)
        else:
            df_new.to_csv(csv_path, mode='a', header=False, index=False)

    def _persist_metrics(self, ticker_dto: TickerDTO, vpin: float, vol: float, regime: Optional[int] = None, regime_confidence: Optional[float] = None) -> None:
        """
        Persist metrics (VPIN + Volatility) to ticker-specific CSV file.
        Uses existing columns: VPIN (uppercase) and vol (lowercase).
        Updates the most recent OHLC row for this ticker with metrics values.
        """
        ticker = ticker_dto.ticker.upper()
        csv_path = self.get_ticker_csv_path(ticker)
        
        try:
            # Read existing CSV (ticker-specific)
            if not os.path.exists(csv_path):
                # Create new CSV with metrics columns using correct names
                new_row = {
                    "event_type": ticker_dto.event_type,
                    "timestamp": ticker_dto.timestamp,
                    "ticker": ticker_dto.ticker,
                    "open": ticker_dto.open,
                    "high": ticker_dto.high,
                    "low": ticker_dto.low,
                    "close": ticker_dto.close,
                    "volume": ticker_dto.volume,
                    "VPIN": vpin,
                    "vol": vol,
                    "regime": regime if regime is not None else None,
                    "regime_confidence": regime_confidence if regime_confidence is not None else None
                }
                df_new = pd.DataFrame([new_row])
                df_new.to_csv(csv_path, index=False)
                return
            
            # Read existing CSV with error handling for malformed rows
            try:
                df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='skip')
            except TypeError:
                # Older pandas versions don't have on_bad_lines, use error_bad_lines
                try:
                    df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                except TypeError:
                    # Even older versions - just skip bad lines silently
                    df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False)
            
            # Ensure VPIN, vol, and regime columns exist (use existing column names)
            if 'VPIN' not in df.columns:
                df['VPIN'] = None
            if 'vol' not in df.columns:
                df['vol'] = None
            if 'regime' not in df.columns:
                df['regime'] = None
            if 'regime_confidence' not in df.columns:
                df['regime_confidence'] = None
            
            # Convert timestamp to datetime for comparison (normalize timezone)
            ticker_timestamp = pd.to_datetime(ticker_dto.timestamp)
            # Normalize to timezone-naive
            if hasattr(ticker_timestamp, 'tz') and ticker_timestamp.tz is not None:
                ticker_timestamp = ticker_timestamp.tz_convert(None)
            elif hasattr(ticker_timestamp, 'tz_localize'):
                try:
                    # Already naive, but ensure it
                    ticker_timestamp = ticker_timestamp.tz_localize(None)
                except:
                    pass
            
            # Safe timestamp parsing for existing data (normalize timezone)
            def safe_parse_ts(ts):
                if pd.isna(ts):
                    return None
                try:
                    parsed = pd.to_datetime(ts, errors='coerce')
                    if pd.isna(parsed):
                        return None
                    # Normalize to timezone-naive for comparison
                    if hasattr(parsed, 'tz') and parsed.tz is not None:
                        parsed = parsed.tz_convert(None)
                    return parsed
                except:
                    return None
            
            updated = False
            
            # Try to find and update existing row
            if 'ticker' in df.columns and 'timestamp' in df.columns and not df.empty:
                # Normalize ticker for comparison (since file is ticker-specific, all rows should match)
                ticker_normalized = ticker_dto.ticker.upper()
                ticker_mask = df['ticker'].str.upper() == ticker_normalized if df['ticker'].dtype == 'object' else df['ticker'] == ticker_normalized
                if ticker_mask.any():
                    ticker_df = df[ticker_mask].copy()
                    # Parse timestamps
                    ticker_df['ts_parsed'] = ticker_df['timestamp'].apply(safe_parse_ts)
                    
                    # Filter valid timestamps
                    valid_mask = ticker_df['ts_parsed'].notna()
                    if valid_mask.any():
                        ticker_df_valid = ticker_df[valid_mask].copy()
                        # Calculate time differences (ensure both are Timestamps)
                        ticker_df_valid['time_diff'] = (
                            pd.to_datetime(ticker_df_valid['ts_parsed']) - ticker_timestamp
                        ).abs()
                        
                        # Find closest match
                        closest_idx = ticker_df_valid['time_diff'].idxmin()
                        time_diff_val = ticker_df_valid.at[closest_idx, 'time_diff']
                        
                        # Update if within 5 minutes
                        if isinstance(time_diff_val, pd.Timedelta) and time_diff_val < pd.Timedelta(minutes=5):
                            df.at[closest_idx, 'VPIN'] = vpin
                            df.at[closest_idx, 'vol'] = vol
                            if regime is not None:
                                df.at[closest_idx, 'regime'] = regime
                            if regime_confidence is not None:
                                df.at[closest_idx, 'regime_confidence'] = regime_confidence
                            updated = True
            
            # If we didn't update, append new row
            if not updated:
                # Create new row with all existing columns
                new_row_dict = {col: None for col in df.columns}
                new_row_dict['timestamp'] = ticker_dto.timestamp
                new_row_dict['ticker'] = ticker_dto.ticker
                new_row_dict['VPIN'] = vpin
                new_row_dict['vol'] = vol
                if regime is not None:
                    new_row_dict['regime'] = regime
                if regime_confidence is not None:
                    new_row_dict['regime_confidence'] = regime_confidence
                
                # Create DataFrame with same columns
                new_row_df = pd.DataFrame([new_row_dict], columns=df.columns)
                df = pd.concat([df, new_row_df], ignore_index=True)
            
            # Save back to ticker-specific CSV
            df.to_csv(csv_path, index=False)
            
        except Exception as e:
            print(f"[CONTROLLER] Error persisting metrics: {str(e)}")
            import traceback
            traceback.print_exc()

    def _persist_article_sentiment(self, news_dto: NewsDTO, sentiment_dto) -> None:
        """Append article with sentiment to articles.csv"""
        new_row = {
            "event_type": "NEWS",
            "timestamp": news_dto.timestamp,
            "ticker": news_dto.ticker,
            "headline": news_dto.headline,
            "url": news_dto.url,
            "sentiment_score": sentiment_dto.sentiment_score,
            "sentiment_label": sentiment_dto.sentiment_label
        }
        df_new = pd.DataFrame([new_row])
        
        if not os.path.exists(self.ARTICLES_CSV_PATH):
            df_new.to_csv(self.ARTICLES_CSV_PATH, index=False)
        else:
            df_new.to_csv(self.ARTICLES_CSV_PATH, mode='a', header=False, index=False)
