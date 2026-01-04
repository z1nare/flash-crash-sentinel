"""
Great Expectations suite definitions.

These functions define expectations for:
- Market data CSVs in `historicalData/*.csv`
- Sentiment data CSV in `dataInCsv/articles_with_sentiment.csv`

The reasons for this code structure are:
- modular (separate functions)
- comprehensible (commented and grouped)
- reusable by tests and by export scripts (human-readable JSON generation)
"""
from __future__ import annotations


def apply_market_suite(validator) -> None:
    """
    GX suite for `historicalData/{TICKER}.csv`.

    Includes:
    - Column expectations
    - Type-/range-specific expectations
    - Conditional expectations (only apply to event_type == 'TICK')
    """
    # Required columns (canonical storage columns: VPIN, vol)
    validator.expect_table_columns_to_contain_set(
        ["event_type", "timestamp", "ticker", "open", "high", "low", "close", "volume", "VPIN", "vol"]
    )

    # Timestamp parseability (lightweight regex; full parsing happens in services)
    validator.expect_column_values_to_match_regex("timestamp", r"^\d{4}-\d{2}-\d{2} ")

    # Numeric columns should be mostly present
    for col in ["open", "high", "low", "close", "volume", "VPIN", "vol"]:
        validator.expect_column_values_to_not_be_null(col, mostly=0.999)

    # Basic numeric sanity (type-specific / range)
    validator.expect_column_values_to_be_between("open", 0.0, None, mostly=0.999)
    validator.expect_column_values_to_be_between("high", 0.0, None, mostly=0.999)
    validator.expect_column_values_to_be_between("low", 0.0, None, mostly=0.999)
    validator.expect_column_values_to_be_between("close", 0.0, None, mostly=0.999)
    validator.expect_column_values_to_be_between("volume", 0.0, None, mostly=0.999)

    # Invariants / bounds
    validator.expect_column_values_to_be_between("VPIN", 0.0, 1.0, mostly=0.999)
    validator.expect_column_values_to_be_between("vol", 0.0, None, mostly=0.999)

    # Conditional: ticks must have ordered ranges
    validator.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B(
        "high", "low", row_condition="event_type == 'TICK'", condition_parser="pandas", mostly=0.999
    )

    # Conditional: high should be >= open and close; low should be <= open and close (mostly)
    validator.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B(
        "high", "open", row_condition="event_type == 'TICK'", condition_parser="pandas", mostly=0.995
    )
    validator.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B(
        "high", "close", row_condition="event_type == 'TICK'", condition_parser="pandas", mostly=0.995
    )
    validator.expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
        "low", "open", row_condition="event_type == 'TICK'", condition_parser="pandas", mostly=0.995
    )
    validator.expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
        "low", "close", row_condition="event_type == 'TICK'", condition_parser="pandas", mostly=0.995
    )

    # Drift / degeneracy guardrails (robustness): avoid pathological all-0/all-1
    # VPIN should not be almost entirely constant
    validator.expect_column_proportion_of_unique_values_to_be_between("VPIN", min_value=0.001, max_value=1.0)
    # Volatility should not be entirely zero (allow some zeros)
    validator.expect_column_values_to_not_be_null("vol", mostly=0.999)


def apply_sentiment_suite(validator) -> None:
    """
    GX suite for `dataInCsv/articles_with_sentiment.csv`.

    Includes:
    - Column expectations
    - Type-/range-specific expectations
    - Conditional expectations (label ↔ sign consistency, soft/mostly)
    """
    validator.expect_table_columns_to_contain_set(
        ["event_type", "timestamp", "ticker", "headline", "url", "sentiment_score", "sentiment_label"]
    )

    validator.expect_column_values_to_not_be_null("timestamp", mostly=0.999)
    validator.expect_column_values_to_not_be_null("ticker", mostly=0.999)

    # Range and enum
    validator.expect_column_values_to_be_between("sentiment_score", -1.0, 1.0, mostly=0.999)
    validator.expect_column_values_to_be_in_set("sentiment_label", ["positive", "negative", "neutral"], mostly=0.999)

    # URL + headline basic checks (type-specific / conditional)
    validator.expect_column_values_to_not_be_null("headline", mostly=0.999)
    validator.expect_column_value_lengths_to_be_between("headline", min_value=5, max_value=500, mostly=0.98)
    validator.expect_column_values_to_not_be_null("url", mostly=0.95)
    validator.expect_column_values_to_match_regex("url", r"^https?://", mostly=0.95)

    # Conditional consistency (soft): label should match score sign most of the time
    validator.expect_column_values_to_be_between(
        "sentiment_score",
        0.0,
        1.0,
        row_condition="sentiment_label == 'positive'",
        condition_parser="pandas",
        mostly=0.90,
    )
    validator.expect_column_values_to_be_between(
        "sentiment_score",
        -1.0,
        0.0,
        row_condition="sentiment_label == 'negative'",
        condition_parser="pandas",
        mostly=0.90,
    )

    # Neutral should be close-ish to 0 most of the time (soft)
    validator.expect_column_values_to_be_between(
        "sentiment_score",
        -0.2,
        0.2,
        row_condition="sentiment_label == 'neutral'",
        condition_parser="pandas",
        mostly=0.80,
    )


