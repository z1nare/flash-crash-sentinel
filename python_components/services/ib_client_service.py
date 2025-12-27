"""
Interactive Brokers API Client Service

Integrates IB API for real-time market data streaming.
Inspired by the provided IB dashboard examples.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Optional, Callable, Dict, List, TYPE_CHECKING
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import get_service_logger
from backend.models.domain import TickerDTO

if TYPE_CHECKING:
    import pandas as pd

logger = get_service_logger("ib_client")

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False
    logger.warning("IB API not available. Install ibapi package: pip install ibapi")

class IBApp(EWrapper, EClient):
    """Interactive Brokers API wrapper."""
    
    def __init__(self):
        if not IB_AVAILABLE:
            raise ImportError("IB API not available. Install: pip install ibapi")
        EClient.__init__(self, self)
        self.connected = False
        self.historical_data = {}
        self.realtime_data = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.ticker_data: Dict[str, Dict] = {}
        
    def error(self, reqId, errorCode, errorString, *args):
        """Handle IB API errors."""
        # Filter out irrelevant warnings
        if errorCode == 2176 and 'fractional share' in errorString.lower():
            return
        if errorCode in [2104, 2106]:  # Market data farm connection messages
            return
        
        logger.debug(f"IB Error {reqId} {errorCode}: {errorString}")
        
        if errorCode >= 2000:  # Serious errors
            logger.error(f"IB Serious Error {errorCode}: {errorString}")
    
    def nextValidId(self, orderId):
        """Connection established."""
        self.connected = True
        logger.info("Connected to Interactive Brokers")
    
    def historicalData(self, reqId, bar):
        """Receive historical data."""
        if reqId not in self.historical_data:
            self.historical_data[reqId] = []
        self.historical_data[reqId].append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        })
    
    def historicalDataEnd(self, reqId, start, end):
        """Historical data request completed."""
        logger.debug(f"Historical data received for reqId {reqId}")
    
    def tickPrice(self, reqId, tickType, price, attrib):
        """Receive real-time price tick."""
        if reqId in self.ticker_data:
            ticker_data = self.ticker_data[reqId]
            if tickType == 1:  # Bid price
                ticker_data['bid'] = price
            elif tickType == 2:  # Ask price
                ticker_data['ask'] = price
            elif tickType == 4:  # Last price
                ticker_data['last'] = price
            
            # Trigger callbacks
            if reqId in self.callbacks:
                for callback in self.callbacks[reqId]:
                    try:
                        callback(ticker_data)
                    except Exception as e:
                        logger.error(f"Error in callback: {e}")
    
    def tickSize(self, reqId, tickType, size):
        """Receive real-time size tick."""
        if reqId in self.ticker_data:
            ticker_data = self.ticker_data[reqId]
            if tickType == 0:  # Bid size
                ticker_data['bid_size'] = size
            elif tickType == 3:  # Ask size
                ticker_data['ask_size'] = size
            elif tickType == 5:  # Last size
                ticker_data['last_size'] = size
                ticker_data['volume'] = size
                ticker_data['timestamp'] = datetime.now()
                
                # Trigger callbacks for volume updates
                if reqId in self.callbacks:
                    for callback in self.callbacks[reqId]:
                        try:
                            callback(ticker_data)
                        except Exception as e:
                            logger.error(f"Error in callback: {e}")

class IBClientService:
    """Service for managing IB API connections and data streaming."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1):
        """
        Initialize IB Client Service.
        
        Args:
            host: IB Gateway/TWS host
            port: Port (7497 for paper trading, 7496 for live)
            client_id: Unique client ID
        """
        if not IB_AVAILABLE:
            logger.warning("IB API not available. Service will not function.")
            self.available = False
            return
        
        self.available = True
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib_app: Optional[IBApp] = None
        self.connection_thread: Optional[threading.Thread] = None
        self.connected = False
        self.subscribed_symbols: Dict[str, int] = {}  # symbol -> reqId
        self.req_id_counter = 1
        self.data_callbacks: Dict[str, Callable] = {}  # symbol -> callback
    
    def connect(self) -> bool:
        """Connect to IB Gateway/TWS."""
        if not self.available:
            logger.error("IB API not available")
            return False
        
        try:
            logger.info(f"Connecting to IB at {self.host}:{self.port}...")
            
            self.ib_app = IBApp()
            
            # Start connection in separate thread
            def connect_thread():
                try:
                    self.ib_app.connect(self.host, self.port, self.client_id)
                    self.ib_app.run()
                except Exception as e:
                    logger.error(f"IB connection error: {e}")
                    self.connected = False
            
            self.connection_thread = threading.Thread(target=connect_thread, daemon=True)
            self.connection_thread.start()
            
            # Wait for connection
            for _ in range(50):
                if self.ib_app and self.ib_app.connected:
                    self.connected = True
                    logger.info("Successfully connected to IB")
                    return True
                time.sleep(0.1)
            
            logger.error("Failed to connect to IB (timeout)")
            return False
            
        except Exception as e:
            logger.error(f"Error connecting to IB: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from IB Gateway."""
        if self.ib_app:
            try:
                self.ib_app.disconnect()
                self.connected = False
                self.subscribed_symbols.clear()
                logger.info("Disconnected from IB")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
    
    def create_equity_contract(self, symbol: str) -> Optional[Contract]:
        """Create equity contract for symbol."""
        if not IB_AVAILABLE:
            return None
        
        contract = Contract()
        contract.symbol = symbol.upper()
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    
    def subscribe_realtime(self, symbol: str, callback: Callable) -> bool:
        """
        Subscribe to real-time market data for a symbol.
        
        Args:
            symbol: Stock ticker (e.g., 'NVDA')
            callback: Function to call with tick data: callback(ticker_dto)
        """
        if not self.connected or not self.ib_app:
            logger.error("Not connected to IB")
            return False
        
        try:
            symbol_upper = symbol.upper()
            
            # Check if already subscribed
            if symbol_upper in self.subscribed_symbols:
                logger.warning(f"{symbol_upper} already subscribed")
                return True
            
            contract = self.create_equity_contract(symbol_upper)
            req_id = self.req_id_counter
            self.req_id_counter += 1
            
            # Store subscription
            self.subscribed_symbols[symbol_upper] = req_id
            self.data_callbacks[symbol_upper] = callback
            
            # Initialize ticker data
            self.ib_app.ticker_data[req_id] = {
                'symbol': symbol_upper,
                'bid': None,
                'ask': None,
                'last': None,
                'volume': None,
                'timestamp': None
            }
            
            # Set up callback
            def data_callback(ticker_data):
                # Convert to TickerDTO when we have enough data
                if ticker_data.get('last') is not None and ticker_data.get('volume') is not None:
                    # Create TickerDTO (using last price for all OHLC if not available)
                    price = ticker_data['last']
                    ticker_dto = TickerDTO(
                        event_type="TICK",
                        timestamp=ticker_data.get('timestamp', datetime.now()),
                        ticker=symbol_upper,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=int(ticker_data.get('volume', 0))
                    )
                    callback(ticker_dto)
            
            if req_id not in self.ib_app.callbacks:
                self.ib_app.callbacks[req_id] = []
            self.ib_app.callbacks[req_id].append(data_callback)
            
            # Request market data
            self.ib_app.reqMktData(req_id, contract, "", False, False, [])
            
            logger.info(f"Subscribed to real-time data for {symbol_upper}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {symbol}: {e}")
            return False
    
    def unsubscribe_realtime(self, symbol: str):
        """Unsubscribe from real-time market data."""
        if not self.connected or not self.ib_app:
            return
        
        symbol_upper = symbol.upper()
        if symbol_upper in self.subscribed_symbols:
            req_id = self.subscribed_symbols[symbol_upper]
            self.ib_app.cancelMktData(req_id)
            del self.subscribed_symbols[symbol_upper]
            if symbol_upper in self.data_callbacks:
                del self.data_callbacks[symbol_upper]
            logger.info(f"Unsubscribed from {symbol_upper}")
    
    def get_historical_data(
        self, 
        symbol: str, 
        duration: str = "1 Y",
        bar_size: str = "1 day"
    ) -> Optional[pd.DataFrame]:
        """
        Request historical data from IB.
        
        Args:
            symbol: Stock ticker
            duration: Duration string (e.g., "1 Y", "6 M")
            bar_size: Bar size (e.g., "1 day", "1 min")
            
        Returns:
            DataFrame with historical data or None
        """
        if not self.connected or not self.ib_app:
            logger.error("Not connected to IB")
            return None
        
        try:
            import pandas as pd
            
            contract = self.create_equity_contract(symbol)
            req_id = self.req_id_counter
            self.req_id_counter += 1
            
            # Clear previous data
            if req_id in self.ib_app.historical_data:
                del self.ib_app.historical_data[req_id]
            
            # Request data
            self.ib_app.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            
            # Wait for data
            timeout = 15
            start_time = time.time()
            while req_id not in self.ib_app.historical_data and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if req_id in self.ib_app.historical_data:
                data = self.ib_app.historical_data[req_id]
                if len(data) > 0:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.rename(columns={
                        'date': 'timestamp',
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume'
                    })
                    logger.info(f"Retrieved {len(df)} historical bars for {symbol}")
                    return df
            
            logger.warning(f"No historical data received for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            return None
    
    def is_market_open(self) -> bool:
        """Check if market is currently open (simplified: 9:30-16:00 ET on weekdays)."""
        now = datetime.now()
        
        # Check if weekday
        if now.weekday() >= 5:  # Saturday/Sunday
            return False
        
        # Check time (simplified - doesn't account for timezone)
        hour = now.hour
        return 9 <= hour < 16
    
    def is_connected(self) -> bool:
        """Check if connected to IB."""
        return self.connected and self.ib_app and self.ib_app.connected

