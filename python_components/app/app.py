import pika
import json
import time
import asyncio # For running async code
from concurrent.futures import ThreadPoolExecutor #  For managing threads
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification # NLP

# CONFIGURATION 
RABBITMQ_HOST = 'localhost'
QUEUE_INPUT = 'news.headlines'
QUEUE_OUTPUT = 'news.sentiment'

# MODEL INITIALIZATION 
# 1. We define the pipeline variable globally, but we initialize it later.
finbert_pipeline = None 
executor = ThreadPoolExecutor(max_workers=4) # Use a pool for blocking tasks (inference)

# --- ASYNCHRONOUS INFERENCE FUNCTION ---
async def async_run_inference(headline):
    """
    A non-blocking function that delegates the heavy, blocking FinBERT call
    to a separate thread managed by the executor.
    """
    # The 'loop' is the current event loop in the main thread.
    loop = asyncio.get_running_loop()
    
    # run_in_executor runs the synchronous prediction function 
    # in the ThreadPoolExecutor, preventing the main thread from blocking.
    result = await loop.run_in_executor(
        executor, 
        lambda: finbert_pipeline(headline)
    )
    
    # Return the raw score and label from the model's output
    return result[0]['score'], result[0]['label']


def init_model():
    """
    Synchronous model loading function.
    """
    global finbert_pipeline
    print("1. Loading FinBERT Model (Blocking IO)...")
    try:
        # Load the components
        tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

        # Create the pipeline once
        finbert_pipeline = pipeline(
            task="text-classification",
            model=model,
            framework="pt",
            tokenizer=tokenizer
        )
        print("Model loaded successfully.")
    except Exception as e:
        print(f"ERROR: Could not load FinBERT model. {e}")
        exit(1)


def callback(ch, method, properties, body):
    """
    The synchronous RabbitMQ callback. It must now launch the async inference.
    """
    # 1. Get the current event loop (or create a new one if none exists)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        # 2. Deserialize the message
        news_event = json.loads(body)
        headline = news_event.get('headline', 'No Headline')
        ticker = news_event.get('ticker', 'MARKET')

        # 3. RUN THE ASYNCHRONOUS TASK HERE
        # We run the async prediction to get the score/label
        score, label = loop.run_until_complete(async_run_inference(headline))

        # 4. Construct and Publish the Sentiment Payload
        sentiment_payload = {
            "ticker": ticker,
            "timestamp": news_event.get('timestamp'),
            "headline": headline,
            "sentiment_score": float(score),
            "sentiment_label": label,
            "type": "SENTIMENT"
        }
        
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_OUTPUT,
            body=json.dumps(sentiment_payload)
        )
        
        ch.basic_ack(delivery_tag=method.delivery_tag) 
        print(f" [✓] SENTIMENT published: {ticker} | {label} ({score:.4f})")

    except Exception as e:
        print(f" [X] Error processing message: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


# --- MAIN RABBITMQ CONSUMER SETUP ---

def start_consumer():
    init_model()
    print(f"2. Connecting to RabbitMQ at {RABBITMQ_HOST}...")
    
    # 4. Initialize the ThreadPoolExecutor before starting consumption
    global executor
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_INPUT)
    channel.queue_declare(queue=QUEUE_OUTPUT)
    
    # Set prefetch count to 1 to prevent queue flooding the slow model
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_INPUT, on_message_callback=callback)

    print('3. NLP Sidecar is listening. Waiting for news headlines...')
    
    try:
        channel.start_consuming()
    finally:
        # 5. Shut down the executor when the consumer stops
        executor.shutdown(wait=False)
        connection.close()


if __name__ == '__main__':
    start_consumer()