"""
Optional FinBERT smoke test.

This test is intentionally skipped by default because it can:
- download a large model on first run
- require network access
- take 30-120s+ depending on machine

Enable explicitly:
  RUN_FINBERT_TESTS=true
and ensure:
  RISKBEACON_DISABLE_FINBERT is not set to "true"
"""

from __future__ import annotations

import os
import pytest

from backend.models.domain import NewsDTO
from services.sentimentService import SentimentService


@pytest.mark.slow
def test_finbert_pipeline_loads_and_scores_text(monkeypatch):
    if os.getenv("RUN_FINBERT_TESTS", "false").lower().strip() != "true":
        pytest.skip("Set RUN_FINBERT_TESTS=true to enable FinBERT smoke test.")

    # Ensure FinBERT isn't disabled
    monkeypatch.delenv("RISKBEACON_DISABLE_FINBERT", raising=False)

    svc = SentimentService()
    if svc.pipeline is None:
        pytest.skip("FinBERT pipeline not available (deps missing or model failed to load).")

    dto = NewsDTO(
        event_type="NEWS",
        timestamp=__import__("datetime").datetime(2026, 1, 1, 12, 0),
        ticker="TEST",
        headline="Markets rally after strong earnings and upbeat guidance.",
        url="https://example.com",
    )

    out = svc.process_news(dto)
    assert out.sentiment_label in {"positive", "negative", "neutral"}
    assert -1.0 <= float(out.sentiment_score) <= 1.0


