from collections import deque
from typing import Optional
from backend.models.domain import TickerDTO
import numpy as np
import sys
import os

# Add utils to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import get_service_logger

logger = get_service_logger("vpin")

class VpinService:
    # Configuration
    BUCKET_VOLUME = 100000.0
    BUCKET_WINDOW = 50
    # Heuristic scale for converting candle return into buy/sell probability.
    # This avoids VPIN saturating at ~1.0 when we only have OHLC bars (not tick-by-tick trade direction).
    PRICE_IMPACT_SCALE = 50.0

    class _VpinState:
        # __slots__ saves memory by preventing the creation of a __dict__ for each instance
        __slots__ = [
            'current_bucket_volume', 
            'current_bucket_imbalance', 
            'imbalance_history', 
            'volume_history', 
            'vpin_ratio_history',  # Store OI/V ratio for each bucket
            'running_vpin_sum',  # Sum of (OI/V) ratios
            'last_vpin'
        ]

        def __init__(self):
            self.current_bucket_volume = 0.0
            self.current_bucket_imbalance = 0.0
            self.imbalance_history = deque()
            self.volume_history = deque()
            # Store the VPIN ratio (OI/V) for each bucket to properly calculate average
            self.vpin_ratio_history = deque()
            self.running_vpin_sum = 0.0
            self.last_vpin = 0.0

    def __init__(self):
        # State dictionary: {ticker_symbol: _VpinState}
        self.state = {}

    def process_tick(self, ticker_dto: TickerDTO) -> Optional[float]:
        """
        Orchestrates VPIN calculation:
        1. Classifies trade direction (Buy/Sell).
        2. Allocates volume to buckets.
        3. Updates rolling window stats.
        Returns: The calculated VPIN score if a bucket was filled, otherwise None.
        """
        ticker = ticker_dto.ticker
        if ticker not in self.state:
            self.state[ticker] = self._VpinState()
        state = self.state[ticker]

        volume = float(ticker_dto.volume)
        if volume <= 0:
            return None

        # 1. Classify Trade Direction (Bulk Volume Classification)
        # NOTE: With aggregated OHLC bars we do NOT know true buy/sell initiated volume.
        # The old hard rule (close>open => 100% buy) makes VPIN saturate near 1.0 and
        # breaks downstream regime detection. We use a smooth heuristic based on
        # candle return to produce a probability in (0,1).
        if ticker_dto.open and ticker_dto.open > 0:
            ret = (float(ticker_dto.close) - float(ticker_dto.open)) / float(ticker_dto.open)
        else:
            ret = 0.0
        buy_ratio = 0.5 + 0.5 * float(np.tanh(ret * self.PRICE_IMPACT_SCALE))
        buy_ratio = max(0.0, min(1.0, buy_ratio))
        
        sell_ratio = 1.0 - buy_ratio

        remaining_volume = volume
        vpin_result = None

        # 2. Fill Buckets
        while remaining_volume > 0:
            space_in_bucket = self.BUCKET_VOLUME - state.current_bucket_volume
            
            # Determine how much of this tick fits in the current bucket
            if remaining_volume >= space_in_bucket:
                chunk_volume = space_in_bucket
                is_bucket_full = True
            else:
                chunk_volume = remaining_volume
                is_bucket_full = False

            # Calculate imbalance for this specific chunk
            chunk_buy = chunk_volume * buy_ratio
            chunk_sell = chunk_volume * sell_ratio
            
            # Update current bucket state
            state.current_bucket_volume += chunk_volume
            state.current_bucket_imbalance += (chunk_buy - chunk_sell)
            
            remaining_volume -= chunk_volume

            # 3. Bucket Filled: Commit to History & Calculate
            if is_bucket_full:
                current_oi = abs(state.current_bucket_imbalance)
                bucket_volume = state.current_bucket_volume
                
                # Calculate VPIN ratio for this bucket: |OI| / V
                # VPIN formula: (1/n) * Σ(|V_i^B - V_i^S| / V_i)
                if bucket_volume > 0:
                    vpin_ratio = current_oi / bucket_volume
                else:
                    vpin_ratio = 0.0
                
                # Manage Sliding Window (Remove old bucket if window is full)
                if len(state.imbalance_history) >= self.BUCKET_WINDOW:
                    removed_oi = state.imbalance_history.popleft()
                    removed_vol = state.volume_history.popleft()
                    removed_ratio = state.vpin_ratio_history.popleft()
                    # Update running sum of VPIN ratios
                    state.running_vpin_sum -= removed_ratio
                
                # Add new bucket data
                state.imbalance_history.append(current_oi)
                state.volume_history.append(bucket_volume)
                state.vpin_ratio_history.append(vpin_ratio)
                
                # Update running sum
                state.running_vpin_sum += vpin_ratio

                # Calculate VPIN as average of ratios: (1/n) * Σ(OI_i / V_i)
                num_buckets = len(state.vpin_ratio_history)
                if num_buckets > 0:
                    state.last_vpin = state.running_vpin_sum / num_buckets
                else:
                    state.last_vpin = 0.0
                
                # Clamp VPIN to valid range [0, 1]
                state.last_vpin = max(0.0, min(1.0, state.last_vpin))
                
                vpin_result = state.last_vpin
                
                logger.debug(
                    f"{ticker}: VPIN={vpin_result:.4f}, "
                    f"Buckets={num_buckets}, "
                    f"Current OI={current_oi:.0f}, "
                    f"Bucket Vol={bucket_volume:.0f}"
                )
                
                # Reset bucket for next accumulation
                state.current_bucket_volume = 0.0
                state.current_bucket_imbalance = 0.0

        return vpin_result