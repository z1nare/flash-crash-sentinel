"""Additional unit tests to boost sentimentService.py coverage."""
import os
from datetime import datetime

import pytest

from backend.models.domain import NewsDTO
from services.sentimentService import SentimentService


def test_sentiment_service_initialization():
    """Test sentiment service initialization."""
    # Disable FinBERT for testing
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    assert service is not None
    assert service.pipeline is None  # Should be None when disabled


def test_process_news_disabled():
    """Test sentiment analysis when FinBERT is disabled."""
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TEST",
        headline="Test headline",
        url="http://test.com"
    )
    result = service.process_news(news)
    
    # Should return neutral when disabled
    assert result is not None
    assert result.sentiment_score == 0.0
    assert result.sentiment_label == "neutral"


def test_process_news_empty_input():
    """Test sentiment analysis with empty/whitespace input."""
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    
    # Empty string
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TEST",
        headline="",
        url="http://test.com"
    )
    result = service.process_news(news)
    assert result.sentiment_score == 0.0
    assert result.sentiment_label == "neutral"
    
    # Whitespace only
    news.headline = "   "
    result = service.process_news(news)
    assert result.sentiment_score == 0.0


def test_process_news_special_characters():
    """Test sentiment analysis with special characters."""
    os.environ["RISKBEACON_DISABLE_FINBERT"] = "true"
    
    service = SentimentService()
    
    # Special characters
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TEST",
        headline="!!! @#$%^&*()",
        url="http://test.com"
    )
    result = service.process_news(news)
    assert result is not None
    assert result.sentiment_score == 0.0  # Neutral when disabled
    
    # Unicode
    news.headline = "测试 中文"
    result = service.process_news(news)
    assert result is not None

