"""
Interactive Brokers (IB) Client Service

This wraps the official `ibapi` EClient/EWrapper in a service-style interface
similar to the other modules in `services/`.

Design goal:
- Keep IB-specific threading/network details isolated here.
- Emit standard `backend.models.domain.TickerDTO` bars into the existing pipeline
  (`ServiceController.process_tick`) so the rest of the project remains unchanged.

Notes:
- `ibapi` is an optional dependency. If it's not installed, this module still
  imports and `IBClientService.available` will be False.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Set, List

from backend.models.domain import TickerDTO
from utils.logger import get_service_logger
from utils.exceptions import ServiceError

logger = get_service_logger("ib_client")

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    _IBAPI_AVAILABLE = True
except Exception:  # pragma: no cover (optional dependency)
    EClient = object  # type: ignore
    EWrapper = object  # type: ignore
    Contract = object  # type: ignore
    _IBAPI_AVAILABLE = False


class IBIntegrationError(ServiceError):
    """Raised when the Interactive Brokers integration fails."""

    def __init__(self, message: str):
        super().__init__(message, service_name="IB_CLIENT_SERVICE")
        self.error_code = "IB_INTEGRATION_ERROR"


@dataclass(frozen=True)
class IBConnectionInfo:
    available: bool
    connected: bool
    streaming: bool
    tickers: List[str]


class _IBApi(EWrapper, EClient):  # type: ignore[misc]
    """
    Internal IB API wrapper.
    Forwards connection state + realtime bars back to IBClientService.
    """

    def __init__(self, owner: "IBClientService"):
        EClient.__init__(self, self)  # type: ignore[misc]
        self._owner = owner

    # --- Connection lifecycle ---
    def nextValidId(self, orderId: int):
        # This is a reliable "we're connected and API is ready" signal.
        self._owner._on_connected()

    def connectionClosed(self):
        self._owner._on_disconnected("connectionClosed")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # Suppress common "informational" messages
        if errorCode in (2104, 2106, 2158, 2176):
            return
        if errorCode == 10167:
            # Delayed market data (no subscription)
            logger.warning("IB delayed market data: %s", errorString)
            return
        logger.warning("IB error reqId=%s code=%s msg=%s", reqId, errorCode, errorString)
        self._owner._on_error(reqId, int(errorCode), str(errorString))

    # --- Real-time bars ---
    def realtimeBar(self, reqId, time_, open_, high, low, close, volume, wap, count):
        # `time_` is epoch seconds (UTC).
        self._owner._on_realtime_bar(
            req_id=int(reqId),
            epoch_seconds=int(time_),
            open_=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=int(volume),
            count=int(count),
        )


class IBClientService:
    """
    Service wrapper around IB Gateway / TWS.

    Usage:
      svc = IBClientService(host, port, client_id, on_bar=controller.process_tick)
      svc.connect()
      svc.start_realtime_bars("NVDA")
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        on_bar: Optional[Callable[[TickerDTO], None]] = None,
    ):
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self._on_bar_callback = on_bar

        self._available = _IBAPI_AVAILABLE
        self._api: Optional[_IBApi] = _IBApi(self) if self._available else None

        self._thread: Optional[threading.Thread] = None
        self._connected_event = threading.Event()
        self._disconnect_event = threading.Event()

        self._lock = threading.Lock()
        self._next_req_id = 1
        self._req_to_ticker: Dict[int, str] = {}
        self._streaming_reqs: Set[int] = set()
        self._last_error: Optional[str] = None

    # --- Properties / status ---
    @property
    def available(self) -> bool:
        return bool(self._available)

    @property
    def connected(self) -> bool:
        return self._connected_event.is_set() and not self._disconnect_event.is_set()

    @property
    def streaming(self) -> bool:
        with self._lock:
            return len(self._streaming_reqs) > 0

    def get_connection_info(self) -> IBConnectionInfo:
        with self._lock:
            tickers = sorted({self._req_to_ticker[r] for r in self._streaming_reqs if r in self._req_to_ticker})
        return IBConnectionInfo(
            available=self.available,
            connected=self.connected,
            streaming=self.streaming,
            tickers=tickers,
        )

    # --- Public API ---
    def set_on_bar_callback(self, cb: Optional[Callable[[TickerDTO], None]]) -> None:
        self._on_bar_callback = cb

    def connect(self, timeout_seconds: float = 5.0) -> bool:
        if not self.available or self._api is None:
            raise IBIntegrationError("ibapi is not installed. Install 'ibapi' to enable IB integration.")

        if self.connected:
            return True

        self._connected_event.clear()
        self._disconnect_event.clear()
        self._last_error = None

        logger.info("Connecting to IB host=%s port=%s clientId=%s", self.host, self.port, self.client_id)

        try:
            self._api.connect(self.host, self.port, clientId=self.client_id)  # type: ignore[union-attr]
        except Exception as e:
            raise IBIntegrationError(f"Failed to connect to IB: {e}")

        # Start network loop in background thread
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        ok = self._connected_event.wait(timeout=timeout_seconds)
        if not ok:
            msg = self._last_error or "Timed out waiting for IB connection (nextValidId)."
            raise IBIntegrationError(msg)

        logger.info("Connected to IB.")
        return True

    def disconnect(self) -> None:
        if not self.available or self._api is None:
            return

        logger.info("Disconnecting from IB...")

        # Stop streams first
        try:
            self.stop_all_streams()
        except Exception:
            pass

        try:
            self._api.disconnect()  # type: ignore[union-attr]
        except Exception:
            pass

        self._disconnect_event.set()
        logger.info("Disconnected from IB.")

    def start_realtime_bars(
        self,
        ticker: str,
        exchange: str = "SMART",
        currency: str = "USD",
        sec_type: str = "STK",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> int:
        """
        Subscribe to IB real-time bars (5-second OHLCV).

        Returns:
            reqId used by IB.
        """
        if not self.connected:
            raise IBIntegrationError("Not connected to IB.")

        t = (ticker or "").strip().upper()
        if not t:
            raise IBIntegrationError("Ticker is required to start streaming.")

        contract = self._create_stock_contract(t, exchange=exchange, currency=currency, sec_type=sec_type)

        with self._lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            self._req_to_ticker[req_id] = t
            self._streaming_reqs.add(req_id)

        logger.info("Starting real-time bars for %s (reqId=%s)", t, req_id)
        try:
            # IB only supports 5-second real-time bars.
            self._api.reqRealTimeBars(req_id, contract, 5, what_to_show, use_rth, [])  # type: ignore[union-attr]
        except Exception as e:
            with self._lock:
                self._streaming_reqs.discard(req_id)
                self._req_to_ticker.pop(req_id, None)
            raise IBIntegrationError(f"Failed to start real-time bars for {t}: {e}")

        return req_id

    def stop_realtime_bars(self, req_id: int) -> None:
        if not self.available or self._api is None:
            return

        with self._lock:
            if req_id not in self._streaming_reqs:
                return
            ticker = self._req_to_ticker.get(req_id, "UNKNOWN")

        logger.info("Stopping real-time bars for %s (reqId=%s)", ticker, req_id)
        try:
            self._api.cancelRealTimeBars(int(req_id))  # type: ignore[union-attr]
        finally:
            with self._lock:
                self._streaming_reqs.discard(req_id)
                self._req_to_ticker.pop(req_id, None)

    def stop_all_streams(self) -> None:
        with self._lock:
            reqs = list(self._streaming_reqs)
        for req_id in reqs:
            self.stop_realtime_bars(req_id)

    # --- Internal helpers ---
    def _run_loop(self) -> None:
        try:
            self._api.run()  # type: ignore[union-attr]
        except Exception as e:
            self._on_disconnected(f"IB run loop crashed: {e}")

    def _create_stock_contract(self, symbol: str, exchange: str, currency: str, sec_type: str) -> "Contract":
        c = Contract()  # type: ignore[call-arg]
        c.symbol = symbol
        c.secType = sec_type
        c.exchange = exchange
        c.currency = currency
        return c

    # --- Callbacks from _IBApi ---
    def _on_connected(self) -> None:
        self._connected_event.set()
        self._disconnect_event.clear()

    def _on_disconnected(self, reason: str) -> None:
        if not self._disconnect_event.is_set():
            logger.warning("IB disconnected: %s", reason)
        self._disconnect_event.set()
        self._connected_event.clear()

    def _on_error(self, req_id: int, error_code: int, message: str) -> None:
        # Capture first error to show in connect timeout failures.
        if not self._connected_event.is_set():
            self._last_error = f"IB error {error_code}: {message}"

    def _on_realtime_bar(
        self,
        req_id: int,
        epoch_seconds: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        count: int,
    ) -> None:
        with self._lock:
            ticker = self._req_to_ticker.get(req_id)

        if not ticker:
            return

        # Convert epoch seconds to naive datetime in local time (consistent with rest of app)
        ts = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).astimezone().replace(tzinfo=None)

        # Emit as the project's standard DTO
        dto = TickerDTO(
            event_type="IB_REALTIME_BAR",
            timestamp=ts,
            ticker=ticker,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=max(0, int(volume)),
        )

        if self._on_bar_callback:
            try:
                self._on_bar_callback(dto)
            except Exception as e:
                # Don't crash IB thread; log and continue.
                logger.exception("Error in on_bar callback for %s: %s", ticker, e)


