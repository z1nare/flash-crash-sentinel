"""
Normalize historical ticker CSVs in `historicalData/`.

This fixes common issues that break plotting/ML pipelines:
- Duplicate metric columns: `VPIN` + `vpin`, `vol` + `volatility`
- Ensures canonical storage columns are `VPIN` and `vol`

Optionally recalculates rolling Yang-Zhang volatility for the entire dataset
and writes it into `vol`.

Usage (from repo root):
  python normalize_historical_data.py --ticker AMD --recalc-vol
  python normalize_historical_data.py --all --recalc-vol
"""

from __future__ import annotations

import argparse
import os
from typing import List

import pandas as pd
import numpy as np

from services.vol_service import VolatilityService


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize duplicate metric columns
    if "VPIN" in df.columns and "vpin" in df.columns:
        df["VPIN"] = df["VPIN"].fillna(df["vpin"])
        df = df.drop(columns=["vpin"], errors="ignore")
    elif "vpin" in df.columns and "VPIN" not in df.columns:
        df["VPIN"] = df["vpin"]
        df = df.drop(columns=["vpin"], errors="ignore")

    if "vol" in df.columns and "volatility" in df.columns:
        df["vol"] = df["vol"].fillna(df["volatility"])
        df = df.drop(columns=["volatility"], errors="ignore")
    elif "volatility" in df.columns and "vol" not in df.columns:
        df["vol"] = df["volatility"]
        df = df.drop(columns=["volatility"], errors="ignore")

    return df


def recalc_vol(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "timestamp" not in df.columns:
        return df

    # Use plotService convention: it expects lowercase names internally, but we store `vol` in CSV.
    work = df.copy()
    # Ensure required cols
    for col in ["open", "high", "low", "close"]:
        if col not in work.columns:
            return df

    # Parse timestamp
    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce", utc=True).dt.tz_convert(None)
    mask = work["timestamp"].notna() & work[["open", "high", "low", "close"]].notna().all(axis=1)
    if mask.sum() < 2:
        df["vol"] = 0.0
        return df

    work_valid = work.loc[mask].copy()
    vols = VolatilityService.calculate_rolling_volatility(work_valid, window=21)
    work_valid["vol"] = vols

    # Write back to original df
    df["vol"] = 0.0
    df.loc[mask, "vol"] = work_valid["vol"].values
    return df


def recalc_vpin_heuristic(df: pd.DataFrame, price_impact_scale: float = 50.0) -> pd.DataFrame:
    """
    Heuristic VPIN-like signal for OHLC bars.
    Produces values in [0,1] based on candle return direction/magnitude.
    """
    df = df.copy()
    for col in ["open", "close"]:
        if col not in df.columns:
            return df
    o = pd.to_numeric(df["open"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    ret = (c - o) / (o.replace(0, np.nan))
    buy_ratio = 0.5 + 0.5 * np.tanh(ret.fillna(0.0) * price_impact_scale)
    vpin_like = (2 * buy_ratio - 1.0).abs().clip(0.0, 1.0)
    df["VPIN"] = vpin_like
    return df


def recalc_regime_rule(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Requires VPIN and vol
    if "VPIN" not in df.columns or "vol" not in df.columns:
        return df
    vpin = pd.to_numeric(df["VPIN"], errors="coerce").fillna(0.0)
    vol = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)

    regime = []
    conf = []
    for v, s in zip(vpin.values, vol.values):
        if v >= 0.8 and s >= 0.03:
            state = 2
            cval = max((v - 0.8) / 0.2, (s - 0.03) / 0.03)
        elif v >= 0.6 or s >= 0.02:
            state = 1
            cval = max((v - 0.6) / 0.4, (s - 0.02) / 0.05)
        else:
            state = 0
            cval = max((0.6 - v) / 0.6, (0.02 - s) / 0.02)
        regime.append(int(state))
        conf.append(float(max(0.0, min(1.0, cval))))

    labels = {0: "Low Vol / Normal", 1: "Correction", 2: "Crash / Liquidity Crisis"}
    df["regime"] = regime
    df["regime_confidence"] = conf
    df["regime_label"] = [labels.get(r, str(r)) for r in regime]
    return df


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Process all tickers in historicalData/")
    parser.add_argument("--ticker", type=str, help="Single ticker to process (e.g., NVDA)")
    parser.add_argument("--recalc-vol", action="store_true", help="Recalculate rolling Yang-Zhang volatility into `vol`")
    parser.add_argument("--recalc-vpin", action="store_true", help="Recalculate VPIN using a bar-based heuristic into `VPIN`")
    parser.add_argument("--recalc-regime", action="store_true", help="Recalculate regime using a rule-based classifier (requires VPIN + vol)")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    hist_dir = os.path.join(base_dir, "historicalData")
    if not os.path.isdir(hist_dir):
        print(f"historicalData directory not found: {hist_dir}")
        return 1

    tickers: List[str] = []
    if args.all:
        tickers = [f.replace(".csv", "") for f in os.listdir(hist_dir) if f.lower().endswith(".csv")]
    elif args.ticker:
        tickers = [args.ticker]
    else:
        print("Provide --all or --ticker TICKER")
        return 2

    for t in tickers:
        path = os.path.join(hist_dir, f"{t.upper()}.csv")
        if not os.path.exists(path):
            print(f"Skip (missing): {path}")
            continue

        print(f"Normalizing: {path}")
        df = pd.read_csv(path, low_memory=False)
        df = normalize_df(df)
        if args.recalc_vpin:
            df = recalc_vpin_heuristic(df)
        if args.recalc_vol:
            df = recalc_vol(df)
        if args.recalc_regime:
            df = recalc_regime_rule(df)
        df.to_csv(path, index=False)
        print(f"✅ Saved: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


