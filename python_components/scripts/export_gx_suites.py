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
from typing import Any, Dict, List, Optional

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


class SuiteRecorder:
    """
    Minimal "validator-like" recorder that captures expectation calls from `gx_suites.py`.

    Why:
    - Exporting suite artifacts should NOT depend on Great Expectations DataContext internals,
      which vary across versions and can break in CI.
    - The portfolio artifacts only need the expectation JSON + readable summary.
    """

    def __init__(self, suite_name: str):
        self.suite_name = suite_name
        self.expectations: List[Dict[str, Any]] = []

    def _add(self, expectation_type: str, kwargs: Dict[str, Any]) -> None:
        self.expectations.append({"expectation_type": expectation_type, "kwargs": kwargs})

    # --- Methods used by test/data_validation/gx_suites.py ---
    def expect_table_columns_to_contain_set(self, columns: List[str]) -> None:
        self._add("expect_table_columns_to_contain_set", {"column_set": columns})

    def expect_column_values_to_match_regex(self, column: str, regex: str, mostly: Optional[float] = None) -> None:
        kwargs: Dict[str, Any] = {"column": column, "regex": regex}
        if mostly is not None:
            kwargs["mostly"] = mostly
        self._add("expect_column_values_to_match_regex", kwargs)

    def expect_column_values_to_not_be_null(self, column: str, mostly: Optional[float] = None) -> None:
        kwargs: Dict[str, Any] = {"column": column}
        if mostly is not None:
            kwargs["mostly"] = mostly
        self._add("expect_column_values_to_not_be_null", kwargs)

    def expect_column_values_to_be_between(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        mostly: Optional[float] = None,
        row_condition: Optional[str] = None,
        condition_parser: Optional[str] = None,
    ) -> None:
        kwargs: Dict[str, Any] = {"column": column, "min_value": min_value, "max_value": max_value}
        if mostly is not None:
            kwargs["mostly"] = mostly
        if row_condition is not None:
            kwargs["row_condition"] = row_condition
        if condition_parser is not None:
            kwargs["condition_parser"] = condition_parser
        self._add("expect_column_values_to_be_between", kwargs)

    def expect_column_pair_values_A_to_be_greater_than_or_equal_to_B(
        self,
        column_A: str,
        column_B: str,
        mostly: Optional[float] = None,
        row_condition: Optional[str] = None,
        condition_parser: Optional[str] = None,
    ) -> None:
        kwargs: Dict[str, Any] = {"column_A": column_A, "column_B": column_B}
        if mostly is not None:
            kwargs["mostly"] = mostly
        if row_condition is not None:
            kwargs["row_condition"] = row_condition
        if condition_parser is not None:
            kwargs["condition_parser"] = condition_parser
        self._add("expect_column_pair_values_A_to_be_greater_than_or_equal_to_B", kwargs)

    def expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
        self,
        column_A: str,
        column_B: str,
        mostly: Optional[float] = None,
        row_condition: Optional[str] = None,
        condition_parser: Optional[str] = None,
    ) -> None:
        kwargs: Dict[str, Any] = {"column_A": column_A, "column_B": column_B}
        if mostly is not None:
            kwargs["mostly"] = mostly
        if row_condition is not None:
            kwargs["row_condition"] = row_condition
        if condition_parser is not None:
            kwargs["condition_parser"] = condition_parser
        self._add("expect_column_pair_values_A_to_be_less_than_or_equal_to_B", kwargs)

    def expect_column_proportion_of_unique_values_to_be_between(
        self, column: str, min_value: float, max_value: float
    ) -> None:
        self._add(
            "expect_column_proportion_of_unique_values_to_be_between",
            {"column": column, "min_value": min_value, "max_value": max_value},
        )

    def expect_column_values_to_be_in_set(self, column: str, value_set: List[str], mostly: Optional[float] = None) -> None:
        kwargs: Dict[str, Any] = {"column": column, "value_set": value_set}
        if mostly is not None:
            kwargs["mostly"] = mostly
        self._add("expect_column_values_to_be_in_set", kwargs)

    def expect_column_value_lengths_to_be_between(
        self, column: str, min_value: int, max_value: int, mostly: Optional[float] = None
    ) -> None:
        kwargs: Dict[str, Any] = {"column": column, "min_value": min_value, "max_value": max_value}
        if mostly is not None:
            kwargs["mostly"] = mostly
        self._add("expect_column_value_lengths_to_be_between", kwargs)

    def to_suite_dict(self) -> Dict[str, Any]:
        return {
            "expectation_suite_name": self.suite_name,
            "expectations": self.expectations,
            "meta": {"exported_by": "scripts/export_gx_suites.py", "suite_type": "code_first"},
        }


def main() -> int:
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(repo_dir, "docs", "data_validation", "gx_suites")

    # Export suites from code-first definitions without relying on GE runtime APIs.
    rec_market = SuiteRecorder("rb_market_data_suite")
    apply_market_suite(rec_market)
    suite_market = rec_market.to_suite_dict()

    rec_sent = SuiteRecorder("rb_sentiment_data_suite")
    apply_sentiment_suite(rec_sent)
    suite_sent = rec_sent.to_suite_dict()

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


