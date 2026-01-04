import os
from typing import Optional

from backend.models.domain import NewsDTO, SentimentDTO
from utils.logger import get_service_logger

logger = get_service_logger("sentiment")

try:
    # Optional heavy deps (CI-friendly): we only require these when FinBERT is enabled.
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification  # type: ignore
    import torch  # type: ignore
except Exception:  # pragma: no cover
    pipeline = None
    AutoTokenizer = None
    AutoModelForSequenceClassification = None
    torch = None

class SentimentService:
    def __init__(self):
        """
        Sentiment model loader.

        CI/Testing safety:
        - Set `RISKBEACON_DISABLE_FINBERT=true` to disable model download/loading.
        - If `transformers`/`torch` are not installed, we fall back to a neutral stub.
        """
        disable = os.getenv("RISKBEACON_DISABLE_FINBERT", "false").lower().strip() == "true"
        self.pipeline = None

        if disable:
            logger.info("FinBERT disabled via RISKBEACON_DISABLE_FINBERT=true (using neutral stub).")
            return

        if pipeline is None or AutoTokenizer is None or AutoModelForSequenceClassification is None or torch is None:
            logger.warning("FinBERT deps not available (transformers/torch missing). Using neutral stub.")
            return

        try:
            logger.info("Loading FinBERT model (ProsusAI/finbert)...")
            tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

            device = 0 if torch.cuda.is_available() else -1
            logger.info("FinBERT device: %s", "cuda" if device == 0 else "cpu")

            self.pipeline = pipeline(
                task="text-classification",
                model=model,
                tokenizer=tokenizer,
                framework="pt",
                device=device,
            )
            logger.info("FinBERT model loaded successfully.")
        except Exception as e:
            # Do not crash the whole app for sentiment: degrade gracefully.
            logger.warning("Could not load FinBERT model; using neutral stub. Error=%s", e)
            self.pipeline = None

    def process_news(self, news_dto: NewsDTO) -> SentimentDTO:
        """
        Process a news item: analyze sentiment and return a detailed DTO.
        """
        text = news_dto.headline
        if not text:
            score, label = 0.0, "neutral"
        else:
            # Run inference if available; otherwise use stub.
            if self.pipeline is None:
                score, label = 0.0, "neutral"
            else:
                result = self.pipeline(text, truncation=True, max_length=512)[0]
                score = float(result["score"])
                label = str(result["label"]).lower()
            
            # Normalize score based on label for easier downstream aggregation
            # Positive -> +Score, Negative -> -Score
            if label == "negative":
                score = -score
            elif label == "neutral":
                score = 0.0

        return SentimentDTO(
            ticker=news_dto.ticker,
            timestamp=news_dto.timestamp,
            headline=news_dto.headline,
            sentiment_score=score,
            sentiment_label=label,
            type="SENTIMENT"
        )

# Usage Example:
if __name__ == "__main__":
    from datetime import datetime
    service = SentimentService()
    
    news = NewsDTO(
        event_type="NEWS",
        timestamp=datetime.now(),
        ticker="TSLA",
        headline="Tesla stock crashes after poor earnings report."
    )
    
    result = service.process_news(news)
    print(f"Analyzed: {result}")
