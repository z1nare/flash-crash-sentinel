import yfinance as yf
import pandas as pd
import pika
import json
import time
import requests
from datetime import datetime, timedelta
import pytz # Recommended for timezone handling

# Configuration 
TICKERS = ["SPY", "TSLA", "NVDA", "AMD", "QQQ", "GLD", "BND", "VTI"]

RABBITMQ_HOST = 'localhost'
QUEUE_TICKS = 'market.ticks'
QUEUE_NEWS = 'news.headlines'
REPLAY_SPEED = 1

GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

TICKER_MAP = {
    "TSLA": "Tesla", "NVDA": "Nvidia", "AMD": "Advanced Micro Devices",
    "SPY": "S&P 500", "QQQ": "Nasdaq", "GLD": "Gold"
}

# RabbitMQ Setup 
def setup_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_TICKS)
        channel.queue_declare(queue=QUEUE_NEWS)
        return connection, channel
    except Exception as e:
        print(f"RabbitMQ Connection Error: {e}")
        return None, None


# Data Fetching Functions 
def fetch_historical_prices(tickers, days_back=2):
    """
    Fetches 1-minute resolution OHLCV data from yfinance.
    """
    print(f"Downloading price data for {tickers}...")
    try:
        data = yf.download(tickers, period=f"{days_back}d", interval="1m", group_by='ticker', auto_adjust=False)
        unified_data = []
        
        for timestamp in data.index:
            for ticker in tickers:
                try:
                    if len(tickers) > 1:
                        row = data[ticker].loc[timestamp]
                    else:
                        row = data.loc[timestamp]
                    
                    if pd.isna(row['Close']): continue

                    unified_data.append({
                        "event_type": "TICK",
                        "timestamp": timestamp,
                        "ticker": ticker,
                        "open": float(row['Open']),   
                        "high": float(row['High']),   
                        "low": float(row['Low']),   
                        "close": float(row['Close']), 
                        "volume": int(row['Volume'])  
                    })
                except KeyError:
                    continue
        return pd.DataFrame(unified_data)
    except Exception as e:
        print(f"YFinance Error: {e}")
        return pd.DataFrame()


def fetch_gdelt_chunk(start_dt, end_dt, entity_query, theme_query):
    """
    Helper to fetch a single chunk of time from GDELT.
    """
    start_str = start_dt.strftime("%Y%m%d%H%M%S")
    end_str = end_dt.strftime("%Y%m%d%H%M%S")
    
    final_query = f'({entity_query}) {theme_query} sourcecountry:US'
    
    params = {
        'query': final_query,
        'mode': 'ArtList',
        'maxrecords': '250',
        'format': 'json',
        'sort': 'DateAsc',
        'startdatetime': start_str,
        'enddatetime': end_str
    }
    
    try:
        resp = requests.get(GDELT_URL, params=params)
        if resp.status_code == 200:
            return resp.json().get('articles', [])
    except Exception as e:
        print(f"  - Error fetching chunk {start_str}-{end_str}: {e}")
    return []


def fetch_gdelt_business_events(start_date, end_date, tickers):
    print(f"Querying GDELT for period: {start_date.date()} to {end_date.date()}")
    
    # 1. Entities Filter
    relevant_names = []
    for t in tickers:
        name = TICKER_MAP.get(t)
        if name:
            relevant_names.append(f'"{name}"' if ' ' in name else name)
    entity_query = " OR ".join(relevant_names)
    
    # 2. Theme Filter (Proxy for CAMEO 010/BUS)
    # Expanded list of themes to catch more relevant financial news
    theme_query = "(theme:ECON_EARNINGSREPORT OR theme:TAX_FNCACT_STATEMENT OR theme:ECON_STOCKMARKET OR theme:ECON_ENTREPRENEURSHIP OR theme:CRISISLEX_CRISISLEXREC)"
    
    # 3. Time-Sliding Window Loop
    # Break the total duration into 6-hour chunks to avoid the 250 record limit
    all_articles = []
    chunk_size = timedelta(hours=6)
    current_pointer = start_date
    
    while current_pointer < end_date:
        next_pointer = min(current_pointer + chunk_size, end_date)
        print(f"  - Fetching window: {current_pointer} -> {next_pointer}...")
        
        articles = fetch_gdelt_chunk(current_pointer, next_pointer, entity_query, theme_query)
        
        for art in articles:
            date_str = art.get('seendate', '').replace('T', '').replace('Z', '')
            if not date_str or len(date_str) < 14: continue
            try:
                ts = datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
            except ValueError:
                continue

            related_ticker = "MARKET"
            title_lower = art.get('title', '').lower()
            for sym, name in TICKER_MAP.items():
                if name.lower() in title_lower:
                    related_ticker = sym
                    break
                    
            all_articles.append({
                "event_type": "NEWS", 
                "timestamp": ts, 
                "ticker": related_ticker,
                "headline": art.get('title', 'No Title'),
                "url": art.get('url', ''),
                "cameo_code": "010", 
                "actor1_code": "BUS",
                "goldstein": 0.0
            })
            
        current_pointer = next_pointer
        time.sleep(1) 

    return pd.DataFrame(all_articles)

# --- Main Execution ---

def run_simulation():
    connection, channel = setup_rabbitmq()
    if not connection: return

    print("Initializing Unified Market Replay Engine...")

    # Use UTC to avoid timezone confusion
    end_time = datetime.now(pytz.utc)
    start_time = end_time - timedelta(days=5) # Last 5 days
    
    df_ticks = fetch_historical_prices(TICKERS, days_back=5)
    df_news = fetch_gdelt_business_events(start_time, end_time, TICKERS)
    
    print(f"\nStats: {len(df_ticks)} price ticks, {len(df_news)} news events.")
    
    # 3. Merge and Sort
    if df_ticks.empty and df_news.empty:
        print("No data found. Exiting.")
        return

    # Normalize timezones
    if not df_ticks.empty and df_ticks['timestamp'].dt.tz is None:
         df_ticks['timestamp'] = df_ticks['timestamp'].dt.tz_localize(pytz.utc)
    elif not df_ticks.empty:
         df_ticks['timestamp'] = df_ticks['timestamp'].dt.tz_convert(pytz.utc)

    if not df_news.empty and df_news['timestamp'].dt.tz is None:
         df_news['timestamp'] = df_news['timestamp'].dt.tz_localize(pytz.utc)
    elif not df_news.empty:
         df_news['timestamp'] = df_news['timestamp'].dt.tz_convert(pytz.utc)
    
    master_timeline = pd.concat([df_ticks, df_news], ignore_index=True)
    master_timeline = master_timeline.sort_values(by='timestamp')

    print(f"\n--- Starting Unified Real-Time Simulation ({len(master_timeline)} events) ---")
    print(f"Speed: 1 event every {REPLAY_SPEED} seconds")
    
    try:
        for _, row in master_timeline.iterrows():
            event = row.to_dict()
            event_timestamp_str = str(event['timestamp'])

            if event['event_type'] == 'TICK':
                rk = QUEUE_TICKS
                payload = {"ticker": event['ticker'], "price": event['price'], "volume": event['volume'], "timestamp": event_timestamp_str, "type": "TRADE"}
                log_msg = f"[TICK] {event['ticker']} @ {event_timestamp_str[11:19]}: ${event['price']:.2f}"

            elif event['event_type'] == 'NEWS':
                rk = QUEUE_NEWS
                payload = {"ticker": event['ticker'], "headline": event['headline'], "url": event['url'], "timestamp": event_timestamp_str, "cameo": event['cameo_code'], "actor": event['actor1_code'], "type": "NEWS"}
                log_msg = f" >>> [NEWS] {event['ticker']}: {event['headline'][:50]}..."
                
            channel.basic_publish(exchange='', routing_key=rk, body=json.dumps(payload))
            print(f" {log_msg}")
            time.sleep(REPLAY_SPEED)

    except KeyboardInterrupt:
        print("\nSimulation Stopped.")
    finally:
        if connection and not connection.is_closed:
            connection.close()

if __name__ == "__main__":
    run_simulation()