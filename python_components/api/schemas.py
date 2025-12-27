"""
API Request/Response Schemas (Pydantic models for FastAPI)
Separate from domain DTOs - these are for HTTP API contracts
"""
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional, Dict, Any

# ========== REQUEST SCHEMAS ==========

class TickRequest(BaseModel):
    """Request schema for /api/tick endpoint"""
    event_type: str = "TICK"
    timestamp: datetime
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "event_type": "TICK",
                "timestamp": "2024-01-15T10:30:00",
                "ticker": "AAPL",
                "open": 150.0,
                "high": 151.5,
                "low": 149.5,
                "close": 150.8,
                "volume": 1000000
            }
        }
    )

class NewsRequest(BaseModel):
    """Request schema for /api/news endpoint"""
    event_type: str = "NEWS"
    timestamp: datetime
    ticker: str
    headline: str
    url: str
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "event_type": "NEWS",
                "timestamp": "2024-01-15T10:30:00",
                "ticker": "AAPL",
                "headline": "Apple reports strong quarterly earnings",
                "url": "https://example.com/news"
            }
        }
    )

# ========== RESPONSE SCHEMAS ==========

class TickResponse(BaseModel):
    """Response schema for /api/tick endpoint"""
    success: bool
    message: str
    vpin_calculated: bool
    vpin_score: float
    volatility_calculated: bool = False
    volatility_score: float = 0.0
    regime: Optional[int] = None
    regime_label: Optional[str] = None
    regime_confidence: Optional[float] = None

class NewsResponse(BaseModel):
    """Response schema for /api/news endpoint"""
    success: bool
    message: str
    ticker: str
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None

class MetricsHistoryResponse(BaseModel):
    """Response schema for /api/metrics/history endpoint"""
    timestamp: datetime
    ticker: str
    vpin: float
    volatility: float
    regime: Optional[int] = None
    regime_label: Optional[str] = None
    regime_confidence: Optional[float] = None

class StatusResponse(BaseModel):
    """Response schema for /api/status endpoint"""
    services_ready: bool
    latest_metrics: Dict[str, Dict[str, Any]]
    vpin_states: Dict[str, Dict[str, Any]]

class PlotGenerateRequest(BaseModel):
    """Request schema for /api/plots/generate endpoint"""
    ticker: Optional[str] = None  # If None, uses most common ticker
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ticker": "NVDA"
            }
        }
    )

class PlotGenerateResponse(BaseModel):
    """Response schema for /api/plots/generate endpoint"""
    success: bool
    message: str
    ticker: str
    plots_generated: Dict[str, str]  # plot_name -> file_path

# Sentiment Analysis Schemas
class SentimentAnalyzeRequest(BaseModel):
    """Request schema for /api/sentiment/analyze endpoint"""
    text: str  # News article or headline text
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "text": "NVIDIA reports record quarterly earnings, stock surges"
            }
        }
    )

class SentimentAnalyzeResponse(BaseModel):
    """Response schema for /api/sentiment/analyze endpoint"""
    success: bool
    sentiment_score: float  # -1 to +1 (negative to positive)
    sentiment_label: str    # positive, negative, or neutral
    text: str               # Input text (truncated if long)

