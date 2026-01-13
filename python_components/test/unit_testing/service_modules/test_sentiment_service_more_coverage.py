"""Additional unit tests to boost sentimentService.py coverage to 80%+."""
import os
from datetime import datetime

import pytest

from backend.models.domain import NewsDTO
from services.sentimentService import SentimentService


def test_sentiment_service_with_deps_unavailable():
    """Test sentiment service when transformers/torch are unavailable."""
    # Temporarily remove transformers if available
    original_disable = os.environ.get("RISKBEACON_DISABLE_FINBERT", "false")
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    try:
        service = SentimentService()
        assert service.pipeline is None
        
        news = NewsDTO(
            event_type="NEWS",
            timestamp=datetime.now(),
            ticker="TEST",
            headline="Test",
            url="http://test.com"
        )
        result = service.process_news(news)
        assert result.sentiment_score == 0.0
        assert result.sentiment_label == "neutral"
    finally:
        os.environ["RISKBEACON_DISABLE_FINBERT"] = original_disable


def test_process_news_long_headline():
    """Test processing news with very long headline."""
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    long_headline = "A" * 1000  # Very long headline
    
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TEST",
        headline=long_headline,
        url="http://test.com"
    )
    
    result = service.process_news(news)
    assert result is not None
    assert result.sentiment_score == 0.0  # Neutral when disabled


def test_process_news_none_headline():
    """Test processing news with empty headline (edge case)."""
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    
    # Pydantic doesn't allow None, so use empty string instead
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TEST",
        headline="",  # Empty string
        url="http://test.com"
    )
    
    result = service.process_news(news)
    assert result.sentiment_score == 0.0
    assert result.sentiment_label == "neutral"


def test_sentiment_service_initialization_paths():
    """Test different initialization paths."""
    # Test with FinBERT disabled (since transformers is available in this env)
    original_disable = os.environ.get("RISKBEACON_DISABLE_FINBERT", "false")
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    try:
        service = SentimentService()
        assert service.pipeline is None  # Should be None when disabled
    finally:
        os.environ["RISKBEACON_DISABLE_FINBERT"] = original_disable

