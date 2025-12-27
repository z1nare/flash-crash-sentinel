from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class TickerDTO(BaseModel):
    event_type: str
    timestamp: datetime
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int

class NewsDTO(BaseModel):
    event_type: str
    timestamp: datetime
    ticker: str
    headline: str
    url: str

class SentimentDTO(BaseModel):
    ticker: str
    timestamp: datetime
    headline: str
    sentiment_score: float
    sentiment_label: str
    type: str = "SENTIMENT"

class RiskMetric(BaseModel):
    ticker: str
    timestamp: datetime
    vpin_score: float
    volatility_score: float
    sentiment_score: float
