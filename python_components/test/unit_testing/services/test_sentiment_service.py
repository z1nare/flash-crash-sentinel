from __future__ import annotations

import os
from datetime import datetime

import pytest

from backend.models.domain import NewsDTO
from services.sentimentService import SentimentService


def _news(headline: str) -> NewsDTO:
    return NewsDTO(
        event_type="NEWS",
        timestamp=datetime(2026, 1, 1, 12, 0),
        ticker="TEST",
        headline=headline,
        url="https://example.com",
    )


def test_sentiment_service_disabled_finbert_is_neutral(monkeypatch):
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    svc = SentimentService()
    out = svc.process_news(_news("Stocks rally on strong earnings"))
    assert out.sentiment_label == "neutral"
    assert out.sentiment_score == 0.0


def test_sentiment_service_empty_headline_is_neutral(monkeypatch):
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    svc = SentimentService()
    out = svc.process_news(_news(""))
    assert out.sentiment_label == "neutral"
    assert out.sentiment_score == 0.0


def test_sentiment_service_pipeline_positive(monkeypatch):
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    svc = SentimentService()

    class _StubPipe:
        def __call__(self, text, truncation=True, max_length=512):
            return [{"label": "positive", "score": 0.8}]

    svc.pipeline = _StubPipe()
    out = svc.process_news(_news("Great outlook for markets"))
    assert out.sentiment_label == "positive"
    assert out.sentiment_score == pytest.approx(0.8)


def test_sentiment_service_pipeline_negative_is_signed(monkeypatch):
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    svc = SentimentService()

    class _StubPipe:
        def __call__(self, text, truncation=True, max_length=512):
            return [{"label": "negative", "score": 0.9}]

    svc.pipeline = _StubPipe()
    out = svc.process_news(_news("Earnings miss triggers selloff"))
    assert out.sentiment_label == "negative"
    assert out.sentiment_score == pytest.approx(-0.9)


def test_sentiment_service_pipeline_neutral_zeroes_score(monkeypatch):
    monkeypatch.setenv("RISKBEACON_DISABLE_FINBERT", "true")
    svc = SentimentService()

    class _StubPipe:
        def __call__(self, text, truncation=True, max_length=512):
            return [{"label": "neutral", "score": 0.99}]

    svc.pipeline = _StubPipe()
    out = svc.process_news(_news("Mixed signals"))
    assert out.sentiment_label == "neutral"
    assert out.sentiment_score == 0.0


