"""
Environment Configuration for RiskBeacon
Manages all configuration through environment variables with sensible defaults
"""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    """Application-wide settings loaded from environment variables"""
    
    # API Configuration
    API_HOST: str = Field(default="0.0.0.0", description="API server host")
    API_PORT: int = Field(default=8000, description="API server port")
    API_BASE_URL: str = Field(default="http://localhost:8000", description="Base URL for API")
    
    # Dashboard Configuration
    DASHBOARD_PORT: int = Field(default=8501, description="Streamlit dashboard port")
    
    # Data Directories
    BASE_DIR: Path = Field(default_factory=lambda: Path(__file__).parent)
    HISTORICAL_DATA_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent / "historicalData",
        description="Directory containing historical market data CSV files"
    )
    DATA_IN_CSV_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent / "dataInCsv",
        description="Directory containing sentiment data"
    )
    MODELS_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent / "experiments" / "regime_detection" / "models",
        description="Directory containing trained ML models"
    )
    PLOTS_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent / "plots",
        description="Directory for generated plot files"
    )
    LOGS_DIR: Path = Field(
        default_factory=lambda: Path(__file__).parent / "logs",
        description="Directory for log files"
    )
    
    # File Paths
    ARTICLES_CSV_PATH: Path = Field(
        default_factory=lambda: Path(__file__).parent / "dataInCsv" / "articles_with_sentiment.csv",
        description="Path to articles with sentiment CSV file"
    )
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)")
    LOG_TO_FILE: bool = Field(default=True, description="Whether to log to files")
    LOG_TO_CONSOLE: bool = Field(default=True, description="Whether to log to console")
    
    # VPIN Configuration
    VPIN_BUCKET_SIZE: int = Field(default=50, description="VPIN bucket size (number of ticks)")
    VPIN_WINDOW: int = Field(default=50, description="VPIN rolling window size")
    
    # Volatility Configuration
    VOLATILITY_WINDOW: int = Field(default=20, description="Volatility calculation window")
    
    # Regime Detection Configuration
    REGIME_MODEL_TYPE: str = Field(default="auto", description="Regime model type (auto, xgboost, random_forest, etc.)")
    REGIME_CONFIDENCE_THRESHOLD: float = Field(default=0.5, description="Minimum confidence for regime predictions")
    
    # Sentiment Configuration
    SENTIMENT_MODEL_NAME: str = Field(
        default="ProsusAI/finbert",
        description="FinBERT model name for sentiment analysis"
    )
    
    # CORS Configuration
    CORS_ORIGINS: list[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS origins (use '*' for development only)"
    )
    
    # Docker/Environment Detection
    ENVIRONMENT: str = Field(default="development", description="Environment (development, production, docker)")
    
    @validator("HISTORICAL_DATA_DIR", "DATA_IN_CSV_DIR", "MODELS_DIR", "PLOTS_DIR", "LOGS_DIR", pre=True)
    def convert_to_path(cls, v):
        """Convert string paths to Path objects"""
        if isinstance(v, str):
            return Path(v)
        return v
    
    @validator("LOG_LEVEL")
    def validate_log_level(cls, v):
        """Validate log level"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            return "INFO"
        return v.upper()
    
    @validator("CORS_ORIGINS", pre=True)
    def parse_cors_origins(cls, v):
        """Parse CORS origins from string or list"""
        if isinstance(v, str):
            if v == "*":
                return ["*"]
            return [origin.strip() for origin in v.split(",")]
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "ignore"  # Ignore extra environment variables
    
    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        dirs = [
            self.HISTORICAL_DATA_DIR,
            self.DATA_IN_CSV_DIR,
            self.MODELS_DIR,
            self.PLOTS_DIR,
            self.LOGS_DIR
        ]
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def get_ticker_csv_path(self, ticker: str) -> Path:
        """Get CSV path for a specific ticker"""
        return self.HISTORICAL_DATA_DIR / f"{ticker.upper()}.csv"
    
    def get_model_path(self, ticker: str) -> Path:
        """Get model path for a specific ticker"""
        return self.MODELS_DIR / f"{ticker.upper()}_best_model.pkl"
    
    def get_metadata_path(self, ticker: str) -> Path:
        """Get metadata path for a specific ticker"""
        return self.MODELS_DIR / f"{ticker.upper()}_metadata.json"


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create global settings instance (singleton pattern)"""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_directories()
    return _settings


# Convenience function to reload settings (useful for testing)
def reload_settings():
    """Reload settings from environment (useful for testing)"""
    global _settings
    _settings = None
    return get_settings()


if __name__ == "__main__":
    # Test configuration loading
    settings = get_settings()
    print("RiskBeacon Configuration:")
    print(f"  API: {settings.API_HOST}:{settings.API_PORT}")
    print(f"  Historical Data: {settings.HISTORICAL_DATA_DIR}")
    print(f"  Models: {settings.MODELS_DIR}")
    print(f"  Log Level: {settings.LOG_LEVEL}")

