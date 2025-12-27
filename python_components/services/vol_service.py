import numpy as np
import pandas as pd
from backend.models.domain import TickerDTO
import os

class VolatilityService:
    def __init__(self, csv_path: str = None):
        """
        Args:
            csv_path: Path to a CSV containing OHLC history. Prefer passing this
                      from `ServiceController` so it points at a ticker file in
                      `historicalData/`.
        """
        if csv_path:
            self.CSV_PATH = csv_path
            return

        # Portable fallback (used only if caller didn't inject a path).
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.CSV_PATH = os.path.join(base_dir, "dataInCsv", "data.csv")
        
    def process_tick(self, ticker_dto: TickerDTO) -> float:
        """
        Process a new tick: load relevant history from CSV and calculate volatility.
        Returns: Calculated Volatility (float)
        """
        print(f"[VOL] Processing tick for {ticker_dto.ticker}")
        
        if not os.path.exists(self.CSV_PATH):
            return 0.0

        try:
            # 1. Load History efficiently with robust error handling
            try:
                df = pd.read_csv(self.CSV_PATH, low_memory=False, on_bad_lines='skip')
            except TypeError:
                # Older pandas versions - use error_bad_lines instead
                try:
                    df = pd.read_csv(self.CSV_PATH, low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                except TypeError:
                    # Even older versions
                    df = pd.read_csv(self.CSV_PATH, low_memory=False, error_bad_lines=False)
            
            # Filter for rows that have OHLC data (not metrics-only rows)
            # Check if required columns exist
            required_ohlc_cols = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_ohlc_cols):
                print(f"[VOL] Warning: Missing OHLC columns in CSV. Returning 0.0")
                return 0.0
            
            # Filter out rows where OHLC data is missing or invalid
            df = df.dropna(subset=required_ohlc_cols, how='any')
            
            if df.empty:
                return 0.0
            
            # Safe timestamp parsing - only parse valid timestamps
            def safe_parse_timestamp(ts):
                """Safely parse timestamp, return None if invalid"""
                if pd.isna(ts):
                    return None
                try:
                    if isinstance(ts, str):
                        parsed = pd.to_datetime(ts, errors='coerce')
                        if pd.isna(parsed):
                            return None
                        # Normalize to timezone-naive for consistency
                        if hasattr(parsed, 'tz') and parsed.tz is not None:
                            parsed = parsed.tz_convert(None)
                        return parsed
                    elif isinstance(ts, (int, float)):
                        # Skip numeric values that aren't timestamps
                        return None
                    else:
                        parsed = pd.to_datetime(ts, errors='coerce')
                        if pd.isna(parsed):
                            return None
                        # Normalize to timezone-naive
                        if hasattr(parsed, 'tz') and parsed.tz is not None:
                            parsed = parsed.tz_convert(None)
                        return parsed
                except Exception:
                    return None
            
            # Apply safe timestamp parsing
            df['timestamp'] = df['timestamp'].apply(safe_parse_timestamp)
            
            # Remove rows with invalid timestamps
            df = df.dropna(subset=['timestamp'])
            
            if df.empty:
                return 0.0
            
            # Filter for specific ticker
            if 'ticker' not in df.columns:
                return 0.0
                
            df_ticker = df[df['ticker'] == ticker_dto.ticker].copy()
            
            if df_ticker.empty:
                return 0.0

            # Ensure OHLC columns are numeric
            for col in required_ohlc_cols:
                df_ticker[col] = pd.to_numeric(df_ticker[col], errors='coerce')
            
            # Remove any rows with invalid numeric data
            df_ticker = df_ticker.dropna(subset=required_ohlc_cols)
            
            if df_ticker.empty:
                return 0.0

            # 2. Resample to Daily Candles
            df_ticker.set_index('timestamp', inplace=True)
            df_ticker = df_ticker.sort_index()
            
            daily_df = df_ticker.resample('D').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            }).dropna()

            # Keep only last 21 days window
            if len(daily_df) > 21:
                daily_df = daily_df.iloc[-21:]
            
            if len(daily_df) < 2:
                print(f"[VOL] Insufficient data: only {len(daily_df)} day(s) available, need at least 2 days for volatility calculation")
                return 0.0

            # 3. Calculate Volatility using all available days (up to 21)
            vol = self._calculate_yang_zhang(daily_df)
            
            print(f"[VOL] Calculated Volatility for {ticker_dto.ticker}: {vol:.6f} (using {len(daily_df)} days)")
            return vol
            
        except Exception as e:
            print(f"[VOL] Error calculating volatility: {str(e)}")
            return 0.0

    def _calculate_yang_zhang(self, history_df: pd.DataFrame) -> float:        
        n = len(history_df)
        if n < 2:
            return 0.0
            
        # Convert to numpy for speed
        opens = history_df['open'].values
        closes = history_df['close'].values
        highs = history_df['high'].values
        lows = history_df['low'].values
        
        # Calculate k constant
        k = 0.34 / (1.34 + (n + 1) / (n - 1))
        
        # 1. Overnight Jump (need at least 2 points for variance)
        if n >= 2:
            log_oj = np.log(opens[1:] / closes[:-1])
            # Check for valid values and sufficient data
            if len(log_oj) > 1 and not np.all(np.isnan(log_oj)):
                var_open_jump = np.nanvar(log_oj, ddof=1)
                if np.isnan(var_open_jump):
                    var_open_jump = 0.0
            else:
                var_open_jump = 0.0
        else:
            var_open_jump = 0.0
        
        # 2. Open to Close
        log_oc = np.log(closes / opens)
        if len(log_oc) > 1 and not np.all(np.isnan(log_oc)):
            var_open_close = np.nanvar(log_oc, ddof=1)
            if np.isnan(var_open_close):
                var_open_close = 0.0
        else:
            var_open_close = 0.0
        
        # 3. Rogers-Satchell
        u = np.log(highs / opens)
        d = np.log(lows / opens)
        c = np.log(closes / opens)
        rs = u * (u - c) + d * (d - c)
        if len(rs) > 0 and not np.all(np.isnan(rs)):
            var_rogers_satchell = np.nanmean(rs)
            if np.isnan(var_rogers_satchell):
                var_rogers_satchell = 0.0
        else:
            var_rogers_satchell = 0.0

        # Combine components
        sigma_sq = var_open_jump + k * var_open_close + (1.0 - k) * var_rogers_satchell
        
        return np.sqrt(sigma_sq) if sigma_sq > 0 else 0.0

    @staticmethod
    def calculate_rolling_volatility(df: pd.DataFrame, window: int = 21) -> np.ndarray:
        """
        Vectorized Rolling Yang-Zhang Calculation.
        1. Resamples Intraday data -> Daily Bars.
        2. Calculates rolling YZ volatility on Daily Bars.
        3. Maps Daily Volatility back to original Intraday timestamps (forward fill).
        """
        # Store original timestamp column BEFORE any operations
        original_timestamps = df['timestamp'].copy()
        
        # Ensure timestamp is datetime (handle mixed timezones)
        if not pd.api.types.is_datetime64_any_dtype(original_timestamps):
            original_timestamps = pd.to_datetime(original_timestamps, errors='coerce', utc=True)
        
        # Convert to timezone-naive for consistency
        if hasattr(original_timestamps.dtype, 'tz') and original_timestamps.dtype.tz is not None:
            original_timestamps = original_timestamps.dt.tz_convert(None)
        elif original_timestamps.dt.tz is not None:
            original_timestamps = original_timestamps.dt.tz_convert(None)

        # Filter out invalid rows
        valid_mask = original_timestamps.notna() & df[['open', 'high', 'low', 'close']].notna().all(axis=1)
        
        if valid_mask.sum() < 2:
            return np.zeros(len(df))

        # 1. Resample to Daily OHLC
        # Create a copy with timestamp as index for resampling
        df_work = df[valid_mask].copy()
        df_work['timestamp'] = original_timestamps[valid_mask]
        df_daily = df_work.set_index('timestamp').resample('D').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()

        if len(df_daily) < window:
            return np.zeros(len(df))

        # 2. Calculate Components Vectorized
        # Log Returns
        log_ho = np.log(df_daily['high'] / df_daily['open'])
        log_lo = np.log(df_daily['low'] / df_daily['open'])
        log_co = np.log(df_daily['close'] / df_daily['open'])
        
        log_oc = np.log(df_daily['close'] / df_daily['open'])
        log_oj = np.log(df_daily['open'] / df_daily['close'].shift(1))
        
        # Rogers-Satchell Term
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)

        # 3. Rolling Variance / Mean
        # N = window size (e.g., 21 days)
        # Variance of Open-Close
        vol_oc = log_oc.rolling(window=window, min_periods=2).var()
        
        # Variance of Overnight Jump
        vol_oj = log_oj.rolling(window=window, min_periods=2).var()
        
        # Mean of RS (RS is already a variance estimator)
        vol_rs = rs.rolling(window=window, min_periods=2).mean()

        # 4. Combine (Standard YZ Formula)
        # k = 0.34 / (1.34 + (n+1)/(n-1))
        n = window
        k = 0.34 / (1.34 + (n + 1) / (n - 1))
        
        yz_variance = vol_oj + k * vol_oc + (1 - k) * vol_rs
        daily_vol = np.sqrt(yz_variance.fillna(0))
        
        # 5. Map back to Original Timestamps
        # Create result array
        result = np.zeros(len(df))
        
        # Normalize dates (set time to 00:00:00 for date matching)
        date_normalized = original_timestamps.dt.normalize()
        
        # Filter out any null timestamps or dates BEFORE creating DataFrame
        valid_mask = date_normalized.notna() & original_timestamps.notna()
        
        if not valid_mask.any():
            return result
        
        # Create DataFrame with original positions (only valid rows)
        df_original = pd.DataFrame({
            'orig_idx': df.index.values[valid_mask],
            'timestamp': original_timestamps.values[valid_mask],
            'date_normalized': date_normalized.values[valid_mask]
        })
        
        # Ensure date_normalized is datetime type
        df_original['date_normalized'] = pd.to_datetime(df_original['date_normalized'])
        df_original['timestamp'] = pd.to_datetime(df_original['timestamp'])
        
        # Remove any rows that became NaT after conversion (safety check)
        df_original = df_original[df_original['date_normalized'].notna()].copy()
        
        if len(df_original) == 0:
            return result
        
        # Sort by date_normalized for merge_asof (not timestamp - we merge on date!)
        df_original = df_original.sort_values('date_normalized')
        
        # Create daily volatility DataFrame - the index of daily_vol is already the date
        daily_vol_df = pd.DataFrame({
            'date': daily_vol.index,
            'vol': daily_vol.values
        })
        
        # Filter out any null dates or volumes
        daily_vol_df = daily_vol_df[daily_vol_df['date'].notna() & daily_vol_df['vol'].notna()].copy()
        
        if len(daily_vol_df) == 0:
            return result
        
        # Ensure date is datetime type
        daily_vol_df['date'] = pd.to_datetime(daily_vol_df['date'])
        
        # Remove any rows that became NaT after conversion (safety check)
        daily_vol_df = daily_vol_df[daily_vol_df['date'].notna()].copy()
        
        if len(daily_vol_df) == 0:
            return result
        
        # Sort by date for merge_asof (required)
        daily_vol_df = daily_vol_df.sort_values('date')
        
        # Use merge_asof to map daily volatility to hourly timestamps
        # Each hour gets the volatility calculated for its day (or the most recent day if no volatility yet)
        # Only include columns needed for merge
        df_merged = pd.merge_asof(
            df_original[['orig_idx', 'date_normalized']],
            daily_vol_df[['date', 'vol']],
            left_on='date_normalized',
            right_on='date',
            direction='backward'  # Each hour gets the latest daily vol up to that point
        )
        
        # Map back to original positions using orig_idx
        for _, row in df_merged.iterrows():
            if pd.notna(row['orig_idx']) and pd.notna(row['vol']):
                orig_idx = int(row['orig_idx'])
                if 0 <= orig_idx < len(result):
                    result[orig_idx] = float(row['vol'])
        
        return result