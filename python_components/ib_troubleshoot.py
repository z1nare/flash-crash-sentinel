"""
Interactive Brokers (IB) Troubleshooting Script

This is a standalone diagnostic tool to debug IB connectivity outside the dashboard.
It helps answer:
- Is the TCP port reachable?
- Is `ibapi` installed?
- Can we complete an IB API handshake (nextValidId)?
- Can we subscribe to real-time bars (5s) and receive data?

Usage examples:
  python ib_troubleshoot.py --host 127.0.0.1 --port 7497 --client-id 1
  python ib_troubleshoot.py --gateway-paper
  python ib_troubleshoot.py --tws-paper
  python ib_troubleshoot.py --gateway-paper --subscribe NVDA --seconds 15
"""

from __future__ import annotations

import argparse
import socket
import sys
import time
from typing import Optional


def tcp_check(host: str, port: int, timeout: float = 2.0) -> tuple[bool, str]:
    """Check if a TCP port is reachable (independent of ibapi)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True, "TCP connect OK"
    except Exception as e:
        return False, f"TCP connect failed: {e}"
    finally:
        try:
            s.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Troubleshoot Interactive Brokers connectivity")

    preset = parser.add_mutually_exclusive_group()
    preset.add_argument("--tws-live", action="store_true", help="Preset: TWS live (port 7496)")
    preset.add_argument("--tws-paper", action="store_true", help="Preset: TWS paper (port 7497)")
    preset.add_argument("--gateway-live", action="store_true", help="Preset: IB Gateway live (port 4001)")
    preset.add_argument("--gateway-paper", action="store_true", help="Preset: IB Gateway paper (port 4002)")

    parser.add_argument("--host", default="127.0.0.1", help="IB host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="IB port (default depends on preset, else 7497)")
    parser.add_argument("--client-id", type=int, default=1, help="IB clientId (default: 1)")
    parser.add_argument("--timeout", type=float, default=5.0, help="Handshake timeout seconds (default: 5)")

    parser.add_argument("--subscribe", type=str, default=None, help="Ticker to subscribe to real-time bars (e.g., NVDA)")
    parser.add_argument("--seconds", type=int, default=10, help="How long to wait after subscribing (default: 10)")

    args = parser.parse_args()

    # Apply preset ports
    if args.port is None:
        if args.tws_live:
            args.port = 7496
        elif args.tws_paper:
            args.port = 7497
        elif args.gateway_live:
            args.port = 4001
        elif args.gateway_paper:
            args.port = 4002
        else:
            args.port = 7497

    host: str = args.host
    port: int = int(args.port)
    client_id: int = int(args.client_id)

    print("=" * 70)
    print("RiskBeacon IB Troubleshooter")
    print("=" * 70)
    print(f"Target: {host}:{port} (clientId={client_id})")
    print()

    # 1) Raw TCP connectivity
    ok, msg = tcp_check(host, port, timeout=2.0)
    print(f"[1/4] TCP connectivity: {msg}")
    if not ok:
        print()
        print("Most common fixes:")
        print("- Ensure TWS / IB Gateway is running and logged in.")
        print("- Verify the 'Socket Port' matches the port you are using.")
        print("- Enable: TWS/Gateway -> API -> Settings -> 'Enable ActiveX and Socket Clients'.")
        print("- If using IB Gateway paper, port is usually 4002 (not 7497).")
        print("- If using TWS paper, port is usually 7497.")
        print("- Check Windows Firewall rules for Java/TWS/IBGateway (allow localhost).")
        print()
        print("Even if TCP fails, continue below if you want to confirm ibapi availability.")
        print()

    # 2) ibapi availability
    try:
        import ibapi  # noqa: F401

        print("[2/4] ibapi import: OK")
        ibapi_available = True
    except Exception as e:
        print(f"[2/4] ibapi import: FAILED ({e})")
        print("Install it with: pip install ibapi")
        ibapi_available = False

    if not ibapi_available:
        print()
        print("Stopping here because ibapi is required for the handshake/subscription tests.")
        return 2

    # 3) IB API handshake
    print("[3/4] IB handshake: attempting connect (waiting for nextValidId)...")
    try:
        from services.ib_client_service import IBClientService

        svc = IBClientService(host=host, port=port, client_id=client_id, on_bar=None)
        svc.connect(timeout_seconds=float(args.timeout))
        info = svc.get_connection_info()
        print(f"Handshake OK: connected={info.connected}, streaming={info.streaming}, tickers={info.tickers}")
    except Exception as e:
        print(f"Handshake FAILED: {e}")
        print()
        print("If you see error 502:")
        print("- Wrong port OR API not enabled in TWS/Gateway OR app not running.")
        print("- Double-check whether you're using TWS vs IB Gateway.")
        print()
        return 3

    # 4) Real-time bars subscription
    if args.subscribe:
        ticker = args.subscribe.strip().upper()
        print(f"[4/4] Realtime bars: subscribing to {ticker} for ~{args.seconds}s")

        received = {"count": 0}

        def _on_bar(dto):
            received["count"] += 1
            if received["count"] <= 3:
                # Print first few bars only
                print(
                    f"Bar {received['count']}: {dto.ticker} {dto.timestamp} "
                    f"O={dto.open:.2f} H={dto.high:.2f} L={dto.low:.2f} C={dto.close:.2f} V={dto.volume}"
                )

        svc.set_on_bar_callback(_on_bar)

        try:
            svc.start_realtime_bars(ticker)
            time.sleep(max(1, int(args.seconds)))
        finally:
            svc.stop_all_streams()
            svc.disconnect()

        if received["count"] == 0:
            print("No bars received.")
            print("Common causes:")
            print("- Market data subscription not enabled (you might get delayed data or nothing).")
            print("- Contract config issues (exchange/currency/secType).")
            print("- Outside market hours (if useRTH is true in subscription).")
            return 4

        print(f"Received {received['count']} bar(s). ✅")
        return 0

    # No subscribe requested; just disconnect cleanly
    svc.disconnect()
    print("[4/4] Realtime bars: skipped (use --subscribe TICKER to test data flow)")
    print("Done. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


