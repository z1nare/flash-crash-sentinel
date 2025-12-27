from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
from backend.models.domain import NewsDTO, SentimentDTO

class SentimentService:
    def __init__(self):
        print("1. Loading FinBERT Model...")
        try:
            tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
            
            # Determine device (GPU if available, else CPU)
            device = 0 if torch.cuda.is_available() else -1
            print(f"   -> Using device: {'GPU (CUDA)' if device == 0 else 'CPU'}")

            self.pipeline = pipeline(
                task="text-classification",
                model=model,
                tokenizer=tokenizer,
                framework="pt",
                device=device
            )
            print("   -> Model loaded successfully.")
        except Exception as e:
            print(f"ERROR: Could not load FinBERT model. {e}")
            raise e

    def process_news(self, news_dto: NewsDTO) -> SentimentDTO:
        """
        Process a news item: analyze sentiment and return a detailed DTO.
        """
        text = news_dto.headline
        if not text:
            score, label = 0.0, "neutral"
        else:
            # Run inference
            result = self.pipeline(text, truncation=True, max_length=512)[0]
            score = float(result['score'])
            label = result['label']
            
            # Normalize score based on label for easier downstream aggregation
            # Positive -> +Score, Negative -> -Score
            if label == 'negative':
                score = -score
            elif label == 'neutral':
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
