"""
Export code-first Great Expectations suites into human-readable JSON + summary markdown.

This supports your coursework requirement:
1) Code-first suites (modular + comprehensible)
2) Afterward, generate readable suite artifacts for auditors

Output:
  docs/data_validation/gx_suites/
    - rb_market_data_suite.json
    - rb_sentiment_data_suite.json
    - SUITE_SUMMARY.md

Run:
  python scripts/export_gx_suites.py
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Tuple

import pandas as pd

import great_expectations as ge

# When executed as a script, Python sets sys.path[0] to the script directory (`scripts/`),
# which breaks imports like `from test...` in CI. Ensure project root is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test.data_validation.gx_suites import apply_market_suite, apply_sentiment_suite


def _write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def _summarize_suite(name: str, suite_dict: Dict[str, Any]) -> List[str]:
    lines = [f"## {name}", ""]
    expectations = suite_dict.get("expectations", [])
    lines.append(f"- Total expectations: **{len(expectations)}**")
    lines.append("")
    for e in expectations:
        etype = e.get("expectation_type", "unknown")
        kwargs = e.get("kwargs", {})
        # Keep it readable: show only key kwargs
        key_kwargs = {k: kwargs.get(k) for k in ["column", "column_A", "column_B", "min_value", "max_value", "mostly", "row_condition"] if k in kwargs}
        lines.append(f"- **{etype}** `{key_kwargs}`")
    lines.append("")
    return lines


def _build_validator_from_df(df: pd.DataFrame, asset_name: str, suite_name: str):
    context = ge.get_context(mode="ephemeral")
    try:
        context.sources.add_pandas(name="rb_runtime")
    except Exception:
        pass

    batch_request = ge.core.batch.RuntimeBatchRequest(
        datasource_name="rb_runtime",
        data_connector_name="runtime_connector",
        data_asset_name=asset_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    # Great Expectations API varies across versions. We want a validator bound to `suite_name`.
    # Some versions expose `add_or_update_expectation_suite`, others use `add_expectation_suite`,
    # and some support `create_expectation_suite=True` on `get_validator`.
    try:
        if hasattr(context, "add_or_update_expectation_suite"):
            context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        elif hasattr(context, "add_expectation_suite"):
            context.add_expectation_suite(expectation_suite_name=suite_name)
    except Exception:
        # If suite creation fails, we rely on get_validator's create_expectation_suite if available.
        pass

    try:
        return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name, create_expectation_suite=True)
    except TypeError:
        # Older/newer GE versions may not accept create_expectation_suite kwarg.
        return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)


def _synthetic_market_df(rows: int = 200) -> pd.DataFrame:
    """
    Build a small market-like DataFrame that satisfies the GX suite expectations.
    This is used in CI when large `historicalData/*.csv` files are not present.
    """
    ts = pd.date_range("2026-01-01 09:30:00", periods=rows, freq="min")
    # Use string timestamps with a space (suite expects regex: YYYY-MM-DD<space>...)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    base = 100.0
    opens = base + (pd.Series(range(rows)) * 0.01)
    closes = opens + 0.02
    highs = closes + 0.10
    lows = opens - 0.10
    volume = pd.Series([1000] * rows)

    # VPIN should not be constant; use a repeating pattern.
    vpin = (pd.Series(range(rows)) % 5) / 10.0 + 0.2  # 0.2..0.6
    vol = pd.Series([0.01 + (i % 3) * 0.001 for i in range(rows)])

    df = pd.DataFrame(
        {
            "event_type": ["TICK"] * rows,
            "timestamp": ts_str,
            "ticker": ["SYNTH"] * rows,
            "open": opens.astype(float),
            "high": highs.astype(float),
            "low": lows.astype(float),
            "close": closes.astype(float),
            "volume": volume.astype(int),
            "VPIN": vpin.astype(float),
            "vol": vol.astype(float),
        }
    )
    return df


def _synthetic_sentiment_df(rows: int = 300) -> pd.DataFrame:
    """
    Build a small sentiment-like DataFrame that satisfies the GX suite expectations.
    Used in CI when the real `articles_with_sentiment.csv` is not present.
    """
    ts = pd.date_range("2026-01-01 10:00:00", periods=rows, freq="min")
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    labels = []
    scores = []
    for i in range(rows):
        if i % 3 == 0:
            labels.append("positive")
            scores.append(0.6)
        elif i % 3 == 1:
            labels.append("negative")
            scores.append(-0.6)
        else:
            labels.append("neutral")
            scores.append(0.0)

    df = pd.DataFrame(
        {
            "event_type": ["NEWS"] * rows,
            "timestamp": ts_str,
            "ticker": ["SYNTH"] * rows,
            "headline": ["Synthetic headline for GX validation."] * rows,
            "url": ["https://example.com/article"] * rows,
            "sentiment_score": scores,
            "sentiment_label": labels,
        }
    )
    return df


def main() -> int:
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(repo_dir, "docs", "data_validation", "gx_suites")

    # Market sample
    hist_dir = os.path.join(repo_dir, "historicalData")
    market_path = None
    for f in ["NVDA.csv", "AMD.csv", "TSLA.csv", "SPY.csv"]:
        p = os.path.join(hist_dir, f)
        if os.path.exists(p):
            market_path = p
            break
    if market_path is None:
        # CI fallback: no large datasets committed
        df_market = _synthetic_market_df()
        market_asset_name = "SYNTHETIC_MARKET"
    else:
        df_market = pd.read_csv(market_path, low_memory=False)
        market_asset_name = os.path.basename(market_path)

    # Sentiment sample
    sentiment_path = os.path.join(repo_dir, "dataInCsv", "articles_with_sentiment.csv")
    if not os.path.exists(sentiment_path):
        df_sent = _synthetic_sentiment_df()
        sentiment_asset_name = "SYNTHETIC_SENTIMENT"
    else:
        df_sent = pd.read_csv(sentiment_path, low_memory=False, nrows=20000)
        sentiment_asset_name = os.path.basename(sentiment_path)

    # Build validators and apply suites
    v_market = _build_validator_from_df(df_market, market_asset_name, "rb_market_data_suite")
    apply_market_suite(v_market)
    suite_market = v_market.get_expectation_suite(discard_failed_expectations=False).to_json_dict()

    v_sent = _build_validator_from_df(df_sent, sentiment_asset_name, "rb_sentiment_data_suite")
    apply_sentiment_suite(v_sent)
    suite_sent = v_sent.get_expectation_suite(discard_failed_expectations=False).to_json_dict()

    # Write JSON
    _write_json(os.path.join(out_dir, "rb_market_data_suite.json"), suite_market)
    _write_json(os.path.join(out_dir, "rb_sentiment_data_suite.json"), suite_sent)

    # Write markdown summary
    summary_lines: List[str] = []
    summary_lines.append("# Great Expectations Suites (Exported)")
    summary_lines.append("")
    summary_lines.append("These suites are generated from code-first definitions in `test/data_validation/gx_suites.py`.")
    summary_lines.append("")
    summary_lines.extend(_summarize_suite("rb_market_data_suite", suite_market))
    summary_lines.extend(_summarize_suite("rb_sentiment_data_suite", suite_sent))

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "SUITE_SUMMARY.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    print(f"✅ Exported GX suites to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


