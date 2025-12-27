import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import numpy as np
from services.vol_service import VolatilityService # Import Logic

# --- NORD THEME COLORS ---
NORD = {
    "bg": "#2E3440",       # Dark Grey Background
    "panel": "#3B4252",    # Lighter Grey Panel
    "grid": "#434C5E",     # Grid Lines
    "text": "#D8DEE9",     # White Text
    "green": "#A3BE8C",    # Safe / Up
    "red": "#BF616A",      # Danger / Down
    "blue": "#88C0D0",     # Volatility
    "orange": "#D08770",   # VPIN / Warning
    "yellow": "#EBCB8B",   # Alert
    "purple": "#B48EAD"    # Sentiment
}

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORICAL_DATA_DIR = os.path.join(BASE_DIR, "..", "historicalData")
DEFAULT_SENTIMENT_PATH = os.path.join(BASE_DIR, "..", "dataInCsv", "articles_with_sentiment.csv")
DEFAULT_OUTPUT_DIR = os.path.join(BASE_DIR, "..", "plots")

def get_ticker_data_path(ticker: str) -> str:
    """
    Get the CSV file path for a specific ticker in historicalData folder.
    
    Args:
        ticker: Stock ticker symbol (e.g., "NVDA", "TSLA")
    
    Returns:
        Full path to ticker-specific CSV file
    """
    return os.path.join(HISTORICAL_DATA_DIR, f"{ticker.upper()}.csv")

def load_data(data_path: str = None, sentiment_path: str = None, ticker: str = None):
    """
    Load and preprocess combined data (Market + Sentiment).
    
    Args:
        data_path: Path to market data CSV. If None and ticker provided, uses ticker-specific file from historicalData/
        sentiment_path: Path to sentiment CSV (defaults to DEFAULT_SENTIMENT_PATH)
        ticker: Specific ticker to load. If data_path is None, will load from historicalData/{ticker}.csv
    
    Returns:
        tuple: (DataFrame, ticker_name)
    """
    # If data_path not provided but ticker is, use ticker-specific file
    if data_path is None:
        if ticker:
            data_path = get_ticker_data_path(ticker)
        else:
            raise ValueError("Either data_path or ticker must be provided")
    
    if sentiment_path is None:
        sentiment_path = DEFAULT_SENTIMENT_PATH
    
    if not os.path.exists(data_path):
        print(f"Error: Data file not found at {data_path}")
        return pd.DataFrame(), None

    print(f"Loading market data from {data_path}...")
    df = pd.read_csv(data_path, low_memory=False)
    df.columns = df.columns.str.lower().str.strip()
    
    # Safe timestamp parsing - handle mixed data types
    def safe_parse_timestamp(ts):
        """Safely parse timestamp, return None if invalid"""
        if pd.isna(ts):
            return None
        try:
            # Skip numeric values that aren't timestamps
            if isinstance(ts, (int, float)):
                # Check if it's a reasonable timestamp (Unix epoch range)
                if ts < 1e9 or ts > 1e12:
                    return None
                # Could be Unix timestamp
                parsed = pd.to_datetime(ts, unit='s', errors='coerce')
            elif isinstance(ts, str):
                # Try parsing as string
                parsed = pd.to_datetime(ts, errors='coerce', utc=True)
            else:
                parsed = pd.to_datetime(ts, errors='coerce', utc=True)
            
            if pd.isna(parsed):
                return None
            
            # Normalize to timezone-naive for consistency
            if hasattr(parsed, 'tz') and parsed.tz is not None:
                parsed = parsed.tz_localize(None)
            
            return parsed
        except:
            return None
    
    # Apply safe timestamp parsing
    df['timestamp'] = df['timestamp'].apply(safe_parse_timestamp)
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['timestamp'])
    
    if df.empty:
        print("Error: No valid timestamps found in data.")
        return pd.DataFrame(), None
    
    df = df.sort_values('timestamp')

    # Check for required columns
    if 'ticker' not in df.columns:
        print("Error: 'ticker' column missing.")
        return pd.DataFrame(), None
    
    # Filter out rows where ticker is NaN or invalid
    df = df[df['ticker'].notna()].copy()
    
    if df.empty:
        print("Error: No valid ticker data found.")
        return pd.DataFrame(), None

    # Use provided ticker (required now, since files are ticker-specific)
    if ticker is None:
        # Try to infer from data if ticker column exists
        if 'ticker' in df.columns:
            target_ticker = df['ticker'].value_counts().idxmax()
        else:
            # Try to infer from filename if data_path was provided
            if data_path:
                filename = os.path.basename(data_path).replace('.csv', '').upper()
                target_ticker = filename
            else:
                raise ValueError("Ticker must be provided when loading ticker-specific data")
    else:
        target_ticker = ticker.upper()

    # Filter by ticker if ticker column exists (should match since file is ticker-specific)
    if 'ticker' in df.columns:
        df = df[df['ticker'].str.upper() == target_ticker].copy() if df['ticker'].dtype == 'object' else df[df['ticker'] == target_ticker].copy()
    
    if df.empty:
        print(f"Error: No data found for ticker '{target_ticker}'")
        return pd.DataFrame(), None

    # Map CSV column names to expected names (VPIN -> vpin, vol -> volatility)
    if 'VPIN' in df.columns and 'vpin' not in df.columns:
        df['vpin'] = df['VPIN']
    if 'vol' in df.columns and 'volatility' not in df.columns:
        df['volatility'] = df['vol']
    
    # Ensure all market columns exist and are numeric
    expected_cols = ['open', 'high', 'low', 'close', 'volume', 'vpin', 'volatility']
    for col in expected_cols:
        if col not in df.columns:
            df[col] = np.nan
        else:
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove rows where essential OHLC data is missing
    required_ohlc = ['open', 'high', 'low', 'close']
    df = df.dropna(subset=required_ohlc, how='all')
    
    if df.empty:
        print("Error: No valid OHLC data found after filtering.")
        return pd.DataFrame(), None

    # Unify Market Data (forward/backward fill for sparse metrics)
    df[expected_cols] = df[expected_cols].ffill().bfill()
    df = df.fillna(0)

    # --- LOAD SENTIMENT ---
    if os.path.exists(sentiment_path):
        print(f"Loading sentiment data from {sentiment_path}...")
        df_sent = pd.read_csv(sentiment_path, low_memory=False)
        
        # Safe timestamp parsing for sentiment data
        def safe_parse_sentiment_ts(ts):
            if pd.isna(ts):
                return None
            try:
                if isinstance(ts, (int, float)):
                    if ts < 1e9 or ts > 1e12:
                        return None
                    parsed = pd.to_datetime(ts, unit='s', errors='coerce')
                elif isinstance(ts, str):
                    parsed = pd.to_datetime(ts, errors='coerce', utc=True)
                else:
                    parsed = pd.to_datetime(ts, errors='coerce', utc=True)
                
                if pd.isna(parsed):
                    return None
                
                if hasattr(parsed, 'tz') and parsed.tz is not None:
                    parsed = parsed.tz_localize(None)
                
                return parsed
            except:
                return None
        
        df_sent['timestamp'] = df_sent['timestamp'].apply(safe_parse_sentiment_ts)
        df_sent = df_sent.dropna(subset=['timestamp'])
        
        # Filter for target ticker
        df_sent = df_sent[df_sent['ticker'] == target_ticker].copy()
        
        if not df_sent.empty:
            df_sent = df_sent.sort_values('timestamp')
            
            # Merge Sentiment into Market Data using merge_asof to align timestamps
            # We want to attach the latest sentiment to the market ticks
            df = pd.merge_asof(df, df_sent[['timestamp', 'sentiment_score', 'headline']], 
                               on='timestamp', 
                               direction='backward',
                               tolerance=pd.Timedelta('1h')) # Only match if within 1 hour
            
            # Fill missing sentiment (no news = 0/neutral)
            df['sentiment_score'] = df['sentiment_score'].fillna(0)
            df['headline'] = df['headline'].fillna('')
        else:
            print(f"No sentiment data found for {target_ticker}")
            df['sentiment_score'] = 0.0
            df['headline'] = ''
    else:
        print("Sentiment CSV not found.")
        df['sentiment_score'] = 0.0
        df['headline'] = ''

    # Recalculate volatility if empty or all zeros
    if df['volatility'].sum() == 0 or (df['volatility'].max() == 0 if df['volatility'].notna().any() else True):
        print("Warning: 'volatility' column is empty. Calculating on-the-fly using VolatilityService...")
        df['volatility'] = VolatilityService.calculate_rolling_volatility(df)
        # Also update vol column for consistency if it exists
        if 'vol' in df.columns:
            df['vol'] = df['volatility']

    return df, target_ticker

def save_plot(fig, filename, output_dir: str = None):
    """
    Save Plotly figure to HTML.
    
    Args:
        fig: Plotly figure object
        filename: Output filename (e.g., "1_sentinel_dashboard.html")
        output_dir: Directory to save plot (defaults to DEFAULT_OUTPUT_DIR)
    
    Returns:
        str: Full path to saved file
    """
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    fig.write_html(path)
    print(f"Saved plot to: {path}")
    return path

def apply_nord_theme(fig, title):
    """Apply professional financial dark theme."""
    fig.update_layout(
        template="plotly_dark",
        title=dict(text=f"<b>{title}</b>", font=dict(size=20, color=NORD['text'])),
        paper_bgcolor=NORD['bg'],
        plot_bgcolor=NORD['panel'],
        font=dict(family="Roboto Mono", color=NORD['text']),
        xaxis=dict(gridcolor=NORD['grid'], showgrid=True),
        yaxis=dict(gridcolor=NORD['grid'], showgrid=True),
        hovermode="x unified",
        margin=dict(l=50, r=50, t=80, b=50)
    )
    return fig

# --- PLOT 1: The Sentinel Dashboard (Master Chart) ---
def plot_sentinel_dashboard(df, ticker, output_dir: str = None):
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.25, 0.25],
        subplot_titles=(f"{ticker} Price Action", "VPIN (Order Flow Toxicity)", "Yang-Zhang Volatility")
    )

    # 1. Price Candles
    fig.add_trace(go.Candlestick(
        x=df['timestamp'], open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name='Price', increasing_line_color=NORD['green'], decreasing_line_color=NORD['red']
    ), row=1, col=1)

    # 2. VPIN Area
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['vpin'], name='VPIN',
        line=dict(color=NORD['orange'], width=2), fill='tozeroy', fillcolor='rgba(208, 135, 112, 0.2)'
    ), row=2, col=1)
    fig.add_hline(y=0.8, line_dash="dot", line_color=NORD['red'], row=2, col=1, annotation_text="CRITICAL RISK")

    # 3. Volatility Line
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['volatility'], name='Volatility',
        line=dict(color=NORD['blue'], width=2)
    ), row=3, col=1)

    apply_nord_theme(fig, f"🛡️ Flash Crash Sentinel: {ticker} Master Dashboard")
    fig.update_layout(height=1000)
    return save_plot(fig, f"{ticker}_1_sentinel_dashboard.html", output_dir)

# --- PLOT 2: Liquidity Stress Heatmap ---
def plot_liquidity_heatmap(df, ticker, output_dir: str = None):
    # Convert Series to list for Plotly Heatmap (z must be 2D array: list of lists)
    vpin_list = df['vpin'].tolist() if hasattr(df['vpin'], 'tolist') else list(df['vpin'].values)
    timestamp_list = df['timestamp'].tolist() if hasattr(df['timestamp'], 'tolist') else list(df['timestamp'].values)
    
    fig = go.Figure(data=go.Heatmap(
        x=timestamp_list,
        y=['Market Stress'],
        z=[vpin_list],  # Must be 2D: list of lists
        colorscale=[[0, NORD['green']], [0.5, NORD['yellow']], [1, NORD['red']]],
        name="Liquidity Stress"
    ))
    apply_nord_theme(fig, f"🔥 Liquidity Stress Heatmap: {ticker}")
    fig.update_layout(height=400, yaxis=dict(showticklabels=False))
    return save_plot(fig, f"{ticker}_2_liquidity_heatmap.html", output_dir)

# --- PLOT 3: Volatility Cone (Price vs Vol) ---
def plot_volatility_cone(df, ticker, output_dir: str = None):
    df['return'] = df['close'].pct_change()
    fig = go.Figure(data=go.Scatter(
        x=df['return'], y=df['volatility'], mode='markers',
        marker=dict(size=8, color=df['vpin'], colorscale='Portland', showscale=True, colorbar=dict(title="VPIN Toxicity")),
        text=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M'),
        hovertemplate="Return: %{x:.2%}<br>Vol: %{y:.4f}<br>Time: %{text}"
    ))
    apply_nord_theme(fig, f"🌪️ Volatility vs. Returns Analysis: {ticker}")
    fig.update_xaxes(title="Price Return", tickformat=".1%")
    fig.update_yaxes(title="Realized Volatility")
    return save_plot(fig, f"{ticker}_3_volatility_cone.html", output_dir)

# --- PLOT 4: Sentiment Overlay (REAL DATA) ---
def plot_sentiment_impact(df, ticker, output_dir: str = None):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Price Line
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['close'], name='Price', 
        line=dict(color=NORD['text'], width=1)
    ), secondary_y=False)
    
    # Filter for significant sentiment events (non-zero)
    sig_sent = df[abs(df['sentiment_score']) > 0.1].copy()
    
    if not sig_sent.empty:
        fig.add_trace(go.Scatter(
            x=sig_sent['timestamp'], 
            y=sig_sent['close'], 
            mode='markers',
            marker=dict(
                size=12, 
                color=sig_sent['sentiment_score'], 
                colorscale='RdYlGn', 
                showscale=True, 
                cmin=-1, cmax=1,
                colorbar=dict(title="Sentiment Score")
            ),
            text=sig_sent['headline'],
            hovertemplate="<b>%{text}</b><br>Score: %{marker.color:.2f}<br>Price: $%{y:.2f}<extra></extra>",
            name='News Impact'
        ), secondary_y=False)
    else:
        print("No significant sentiment events found to plot.")

    apply_nord_theme(fig, f"📰 News Sentiment Impact: {ticker}")
    return save_plot(fig, f"{ticker}_4_sentiment_impact.html", output_dir)

# --- PLOT 5: Crash Probability Gauge (Updated with Real Sentiment) ---
def plot_crash_probability(df, ticker, output_dir: str = None):
    # Safety checks for empty or invalid data
    if df.empty or len(df) == 0:
        print(f"Warning: Empty dataframe for crash probability plot for {ticker}")
        return None
    
    # Ensure required columns exist and are numeric
    for col in ['volatility', 'vpin', 'sentiment_score']:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # Normalize volatility with safety checks
    vol_min = df['volatility'].min()
    vol_max = df['volatility'].max()
    vol_range = vol_max - vol_min
    
    if pd.isna(vol_range) or vol_range == 0 or pd.isna(vol_min) or pd.isna(vol_max):
        vol_norm = pd.Series([0.0] * len(df), index=df.index)
    else:
        vol_norm = (df['volatility'] - vol_min) / (vol_range + 1e-9)
        vol_norm = vol_norm.fillna(0.0)
    
    # Composite Score: VPIN (50%) + Vol (30%) + Negative Sentiment (20%)
    # Convert sentiment (-1 to 1) to risk (0 to 1), where -1 is high risk
    # Risk = (1 - sentiment) / 2 -> -1 becomes 1.0 (Risk), +1 becomes 0.0 (Safe)
    sent_risk = (1 - df['sentiment_score']) / 2
    sent_risk = pd.to_numeric(sent_risk, errors='coerce').fillna(0.0)
    
    # Calculate crash probability
    df['crash_prob'] = (df['vpin'] * 0.5) + (vol_norm * 0.3) + (sent_risk * 0.2)
    df['crash_prob'] = pd.to_numeric(df['crash_prob'], errors='coerce').fillna(0.0).clip(0.0, 1.0)
    
    last_val = float(df['crash_prob'].iloc[-1]) if not df.empty else 0.0
    prev_val = float(df['crash_prob'].iloc[-2]) if len(df) > 1 else 0.0
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta", value = last_val * 100,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "<b>Crash Probability Index</b>", 'font': {'size': 24, 'color': NORD['text']}},
        delta = {'reference': prev_val * 100},
        gauge = {
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': NORD['text']},
            'bar': {'color': NORD['orange']},
            'bgcolor': NORD['bg'], 'borderwidth': 2, 'bordercolor': NORD['grid'],
            'steps': [{'range': [0, 50], 'color': NORD['green']}, {'range': [50, 80], 'color': NORD['yellow']}, {'range': [80, 100], 'color': NORD['red']}],
            'threshold': {'line': {'color': "white", 'width': 4}, 'thickness': 0.75, 'value': 90}
        }
    ))
    apply_nord_theme(fig, f"⚠️ Live Crash Probability: {ticker}")
    return save_plot(fig, f"{ticker}_5_crash_gauge.html", output_dir)

def generate_all_plots(data_path: str = None, sentiment_path: str = None, ticker: str = None, output_dir: str = None):
    """
    Generate all plots and return their file paths.
    
    Args:
        data_path: Path to market data CSV
        sentiment_path: Path to sentiment CSV
        ticker: Specific ticker to plot (None = most common)
        output_dir: Directory to save plots
    
    Returns:
        dict: Mapping of plot names to file paths
    """
    df, target_ticker = load_data(data_path, sentiment_path, ticker)
    if df.empty:
        return {}
    
    print(f"Generating plots for {target_ticker}...")
    
    # Update save_plot calls to use output_dir
    plot_paths = {}
    
    # Temporarily override save_plot to capture paths
    original_save = save_plot
    saved_paths = []
    
    def save_and_capture(fig, filename):
        path = original_save(fig, filename, output_dir)
        saved_paths.append((filename, path))
        return path
    
    # Generate all plots
    plot_sentinel_dashboard(df, target_ticker, output_dir)
    plot_liquidity_heatmap(df, target_ticker, output_dir)
    plot_volatility_cone(df, target_ticker, output_dir)
    plot_sentiment_impact(df, target_ticker, output_dir)
    plot_crash_probability(df, target_ticker, output_dir)
    
    # Build return dict with ticker-prefixed filenames
    plot_files = [
        f"{target_ticker}_1_sentinel_dashboard.html",
        f"{target_ticker}_2_liquidity_heatmap.html",
        f"{target_ticker}_3_volatility_cone.html",
        f"{target_ticker}_4_sentiment_impact.html",
        f"{target_ticker}_5_crash_gauge.html"
    ]
    
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    
    for filename in plot_files:
        full_path = os.path.join(output_dir, filename)
        if os.path.exists(full_path):
            plot_paths[filename] = full_path
    
    print(f"Done! Generated {len(plot_paths)} plots.")
    return plot_paths

def main():
    """CLI entry point for standalone execution."""
    plot_paths = generate_all_plots()
    if plot_paths:
        print(f"Generated {len(plot_paths)} plots:")
        for name, path in plot_paths.items():
            print(f"  - {name}: {path}")
    else:
        print("No plots generated. Check data files.")

if __name__ == "__main__":
    main()
