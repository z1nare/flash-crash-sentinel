"""
Custom Exception Classes for RiskBeacon
Provides structured error handling across the application
"""


class RiskBeaconException(Exception):
    """Base exception for all RiskBeacon errors"""
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        self.message = message
        self.error_code = error_code or "RISKBEACON_ERROR"
        self.details = details or {}
        super().__init__(self.message)


class DataValidationError(RiskBeaconException):
    """Raised when data validation fails"""
    def __init__(self, message: str, field: str = None, value: any = None):
        details = {}
        if field:
            details["field"] = field
        if value is not None:
            details["invalid_value"] = str(value)
        super().__init__(message, "DATA_VALIDATION_ERROR", details)


class ServiceError(RiskBeaconException):
    """Raised when a service operation fails"""
    def __init__(self, message: str, service_name: str = None):
        details = {}
        if service_name:
            details["service"] = service_name
        super().__init__(message, "SERVICE_ERROR", details)


class VPINCalculationError(ServiceError):
    """Raised when VPIN calculation fails"""
    def __init__(self, message: str, ticker: str = None):
        details = {}
        if ticker:
            details["ticker"] = ticker
        super().__init__(message, "VPIN_SERVICE")
        self.error_code = "VPIN_CALCULATION_ERROR"


class VolatilityCalculationError(ServiceError):
    """Raised when volatility calculation fails"""
    def __init__(self, message: str, ticker: str = None):
        details = {}
        if ticker:
            details["ticker"] = ticker
        super().__init__(message, "VOLATILITY_SERVICE")
        self.error_code = "VOLATILITY_CALCULATION_ERROR"


class SentimentAnalysisError(ServiceError):
    """Raised when sentiment analysis fails"""
    def __init__(self, message: str, headline: str = None):
        details = {}
        if headline:
            details["headline_preview"] = headline[:100] if len(headline) > 100 else headline
        super().__init__(message, "SENTIMENT_SERVICE")
        self.error_code = "SENTIMENT_ANALYSIS_ERROR"


class RegimeDetectionError(ServiceError):
    """Raised when regime detection fails"""
    def __init__(self, message: str, ticker: str = None, model_path: str = None):
        details = {}
        if ticker:
            details["ticker"] = ticker
        if model_path:
            details["model_path"] = model_path
        super().__init__(message, "REGIME_SERVICE")
        self.error_code = "REGIME_DETECTION_ERROR"


class FileNotFoundError(RiskBeaconException):
    """Raised when a required file is not found"""
    def __init__(self, message: str, file_path: str = None):
        details = {}
        if file_path:
            details["file_path"] = file_path
        super().__init__(message, "FILE_NOT_FOUND", details)


class ConfigurationError(RiskBeaconException):
    """Raised when configuration is invalid or missing"""
    def __init__(self, message: str, config_key: str = None):
        details = {}
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, "CONFIGURATION_ERROR", details)


class ModelLoadingError(RiskBeaconException):
    """Raised when ML model cannot be loaded"""
    def __init__(self, message: str, model_path: str = None, ticker: str = None):
        details = {}
        if model_path:
            details["model_path"] = model_path
        if ticker:
            details["ticker"] = ticker
        super().__init__(message, "MODEL_LOADING_ERROR", details)


class DataProcessingError(RiskBeaconException):
    """Raised when data processing fails"""
    def __init__(self, message: str, data_source: str = None):
        details = {}
        if data_source:
            details["data_source"] = data_source
        super().__init__(message, "DATA_PROCESSING_ERROR", details)


class PlotGenerationError(RiskBeaconException):
    """Raised when plot generation fails"""
    def __init__(self, message: str, plot_type: str = None, ticker: str = None):
        details = {}
        if plot_type:
            details["plot_type"] = plot_type
        if ticker:
            details["ticker"] = ticker
        super().__init__(message, "PLOT_GENERATION_ERROR", details)

