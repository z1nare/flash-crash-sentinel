"""
RiskBeacon - Streamlit Dashboard
Bloomberg Terminal Theme

Main dashboard application connecting to FastAPI backend
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import streamlit.components.v1 as components
import re
from utils.logger import get_dashboard_logger

logger = get_dashboard_logger()

# Import custom modules
import sys
import os
# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from frontend.api_client import APIClient
from frontend.bloomberg_theme import apply_theme
from frontend.components import render_regime_light, render_metric_box, render_news_feed, render_status_indicator
from frontend.charts import (
    create_candlestick_chart,
    create_volatility_chart,
    create_sentiment_timeline,
    create_metrics_comparison
)

# Import data replay service for automatic simulation
try:
    from services.data_replay_service import get_replay_service, start_replay, stop_replay, is_replay_running
    REPLAY_AVAILABLE = True
except ImportError:
    REPLAY_AVAILABLE = False
    logger.info("Data replay service not available (Excel files may not be accessible)")

# Page configuration
st.set_page_config(
    page_title="RiskBeacon",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Bloomberg theme
apply_theme()

# Initialize API client
@st.cache_resource
def get_api_client():
    return APIClient()

api_client = get_api_client()

# Initialize session state
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = False
if 'refresh_interval' not in st.session_state:
    st.session_state.refresh_interval = 5

# Sidebar
with st.sidebar:
    st.title("⚡ RiskBeacon")
    st.markdown("---")
    
    # API Status
    st.subheader("Connection Status")
    api_online = api_client.check_connection()
    render_status_indicator(api_online)
    
    # IB Integration Status (Optional - shows if IB service is available)
    if api_online:
        try:
            status = api_client.get_status()
            ib_connected = False
            ib_available = False
            
            if status and isinstance(status, dict):
                if 'ib_connection' in status and status['ib_connection']:
                    ib_connection = status['ib_connection']
                    ib_connected = ib_connection.get('connected', False)
                    ib_available = ib_connection.get('available', False)
            
            st.markdown("**Interactive Brokers:**")
            
            if ib_available:
                if ib_connected:
                    st.success("🟢 IB Connected")
                    st.caption("Real-time data enabled")
                    if st.button("Disconnect IB", use_container_width=True, key="ib_disconnect"):
                        result = api_client.disconnect_ib()
                        if result.get('success'):
                            st.success("Disconnected from IB")
                            st.rerun()
                        else:
                            st.error(f"Error: {result.get('error', 'Unknown error')}")
                else:
                    st.info("⚪ IB Not Connected")
                    st.caption("Using historical data only")
                    # Allow overriding connection params (common issue: IB Gateway paper uses 4002)
                    with st.expander("IB Connection Settings", expanded=False):
                        ib_host = st.text_input("Host", value="127.0.0.1", key="ib_host")
                        ib_port = st.number_input("Port", min_value=1, max_value=65535, value=7497, step=1, key="ib_port")
                        ib_client_id = st.number_input("Client ID", min_value=0, max_value=9999, value=1, step=1, key="ib_client_id")
                    if st.button("Connect to IB", use_container_width=True, key="ib_connect"):
                        with st.spinner("Connecting to IB Gateway..."):
                            result = api_client.connect_ib(host=ib_host, port=int(ib_port), client_id=int(ib_client_id))
                            if result.get('success'):
                                st.success("✅ Connected to IB!")
                                st.rerun()
                            else:
                                error_msg = result.get('error', result.get('detail', 'Unknown error'))
                                st.error(f"Connection failed: {error_msg}")
                                st.info("💡 Common ports: TWS paper=7497, TWS live=7496, IB Gateway paper=4002, IB Gateway live=4001.")
            else:
                st.info("⚪ IB Not Available")
        except Exception as e:
            # IB status check not available or failed
            pass
    
    st.markdown("---")
    
    # Ticker Selection
    st.subheader("Ticker Selection")
    if api_online:
        tickers = api_client.list_tickers()
        if not tickers:
            tickers = ["NVDA", "TSLA", "AMD", "SPY"]  # Fallback
    else:
        tickers = ["NVDA", "TSLA", "AMD", "SPY"]  # Fallback
    
    # Filter out TEST and INTEGRATION_TEST tickers
    tickers = [t for t in tickers if t.upper() not in ['TEST', 'INTEGRATION_TEST', 'INTEGRATIONTEST']]
    
    if not tickers:
        tickers = ["NVDA", "TSLA", "AMD", "SPY"]  # Fallback if all filtered out
    
    selected_ticker = st.selectbox(
        "Select Ticker",
        options=tickers,
        index=0 if "NVDA" in tickers else 0
    )
    
    st.markdown("---")
    
    # Time Range
    st.subheader("Time Range")
    time_range = st.selectbox(
        "Data Range",
        options=["Last 100", "Last 500", "Last 1000", "All"],
        index=1
    )
    
    limit_map = {
        "Last 100": 100,
        "Last 500": 500,
        "Last 1000": 1000,
        "All": 10000
    }
    data_limit = limit_map[time_range]
    
    st.markdown("---")
    
    # Auto Refresh
    st.subheader("Auto Refresh")
    auto_refresh = st.checkbox("Enable Auto Refresh", value=False)
    
    # Data Replay Simulation (automatic when auto-refresh is enabled)
    if REPLAY_AVAILABLE and api_online:
        st.markdown("**Data Simulation:**")
        replay_running = is_replay_running()
        
        if auto_refresh:
            # Determine which tickers to replay
            # Use selected ticker + common tickers (NVDA, TSLA, AMD) if available
            replay_tickers = [selected_ticker.upper()]
            common_tickers = ["NVDA", "TSLA", "AMD"]
            for ticker in common_tickers:
                if ticker.upper() != selected_ticker.upper() and ticker.upper() in [t.upper() for t in tickers]:
                    replay_tickers.append(ticker.upper())
            
            # Remove duplicates while preserving order
            seen = set()
            replay_tickers = [x for x in replay_tickers if not (x in seen or seen.add(x))]
            
            # Start replay if not already running
            if not replay_running:
                try:
                    if start_replay(replay_tickers):
                        st.success(f"🟢 Simulating: {', '.join(replay_tickers)}")
                        st.caption("Sending Bloomberg data to API")
                    else:
                        st.warning("⚠️ Could not start data simulation")
                        st.caption("Check Excel files in BloombergData/")
                except Exception as e:
                    st.warning(f"⚠️ Simulation error: {str(e)}")
            else:
                st.info(f"🟡 Simulation running: {', '.join(replay_tickers)}")
                st.caption("Sending data from Excel files")
        else:
            # Stop replay when auto-refresh is disabled
            if replay_running:
                try:
                    stop_replay()
                    st.info("⚪ Simulation stopped")
                except Exception as e:
                    st.warning(f"⚠️ Error stopping simulation: {str(e)}")
            else:
                st.info("⚪ Simulation inactive")
                st.caption("Enable auto-refresh to start")
        
        st.markdown("---")
    
    if auto_refresh:
        refresh_interval = st.slider("Refresh Interval (seconds)", 1, 60, 5)
        st.session_state.auto_refresh = True
        st.session_state.refresh_interval = refresh_interval
    else:
        st.session_state.auto_refresh = False
    
    st.markdown("---")
    
    # Manual Refresh Button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")

# Main Dashboard
st.title("📊 RiskBeacon Dashboard")
st.markdown("**Real-time Risk Monitoring**")

# Get data
if api_online:
    # Fetch metrics history
    with st.spinner("Loading metrics..."):
        metrics_df = api_client.get_metrics_history(selected_ticker, limit=data_limit)
    
    # Fetch market data - try direct CSV read first
    with st.spinner("Loading market data..."):
        market_df = api_client.get_market_data(selected_ticker)
        # If empty, we'll use metrics_df below (which has OHLC from API)
    
    # Fetch sentiment data
    with st.spinner("Loading sentiment data..."):
        sentiment_df = api_client.get_sentiment_data()
        if not sentiment_df.empty and 'ticker' in sentiment_df.columns:
            sentiment_df = sentiment_df[sentiment_df['ticker'].str.upper() == selected_ticker.upper()]
    
    # Get latest metrics
    latest_metrics = {}
    if not metrics_df.empty:
        latest = metrics_df.iloc[0]
        latest_metrics = {
            'vpin': latest.get('vpin', latest.get('VPIN', 0.0)),
            'volatility': latest.get('volatility', latest.get('vol', 0.0)),
            'regime': latest.get('regime'),
            'regime_label': latest.get('regime_label'),
            'regime_confidence': latest.get('regime_confidence')
        }
    
    # Get status
    status = api_client.get_status()
    latest_metrics_status = {}
    if status and 'latest_metrics' in status and selected_ticker.upper() in status['latest_metrics']:
        latest_metrics_status = status['latest_metrics'][selected_ticker.upper()]
        # Merge with metrics_df data
        if latest_metrics_status:
            latest_metrics.update(latest_metrics_status)
    
    # If market_df is empty, use metrics_df for market data (it has OHLC from API)
    if market_df.empty and not metrics_df.empty:
        # Use metrics_df as market data if it has OHLC columns
        if all(col in metrics_df.columns for col in ['open', 'high', 'low', 'close']):
            market_df = metrics_df.copy()
            # Ensure VPIN and vol columns exist
            if 'VPIN' not in market_df.columns and 'vpin' in market_df.columns:
                market_df['VPIN'] = market_df['vpin']
            if 'vol' not in market_df.columns and 'volatility' in market_df.columns:
                market_df['vol'] = market_df['volatility']
    
    # If still empty, try direct CSV read as last resort
    if market_df.empty:
        market_df = api_client.get_market_data(selected_ticker)
        # Ensure VPIN and vol columns exist
        if not market_df.empty:
            if 'VPIN' not in market_df.columns and 'vpin' in market_df.columns:
                market_df['VPIN'] = market_df['vpin']
            if 'vol' not in market_df.columns and 'volatility' in market_df.columns:
                market_df['vol'] = market_df['volatility']
else:
    metrics_df = pd.DataFrame()
    market_df = pd.DataFrame()
    sentiment_df = pd.DataFrame()
    latest_metrics = {}

# Top Row: Regime Light and Key Metrics
col1, col2, col3, col4, col5 = st.columns([1.5, 1, 1, 1, 1])

with col1:
    st.subheader("Market Regime")
    regime = latest_metrics.get('regime')
    regime_confidence = latest_metrics.get('regime_confidence', 0.0)
    
    # Try to get regime from metrics_df if not in latest_metrics
    if regime is None and not metrics_df.empty:
        # Check all rows for regime data (not just first) - look for most recent non-null regime
        if 'regime' in metrics_df.columns:
            regime_rows = metrics_df[metrics_df['regime'].notna()]
            if not regime_rows.empty:
                # Get the most recent row with regime data
                regime_val = regime_rows.iloc[0].get('regime')
                # Handle both numeric and string regime values
                if pd.notna(regime_val):
                    try:
                        regime = int(float(regime_val))  # Convert to int
                    except (ValueError, TypeError):
                        regime = None
                else:
                    regime = None
                if regime is not None:
                    regime_confidence = regime_rows.iloc[0].get('regime_confidence', 0.0)
                    if regime_confidence is None or pd.isna(regime_confidence):
                        regime_confidence = 0.0
                    # Also get regime_label if available
                    if 'regime_label' in regime_rows.columns:
                        regime_label_val = regime_rows.iloc[0].get('regime_label')
                        if pd.notna(regime_label_val) and regime_label_val not in latest_metrics:
                            latest_metrics['regime_label'] = regime_label_val
        # If still None, try regime_label column
        if regime is None and 'regime_label' in metrics_df.columns:
            regime_label_rows = metrics_df[metrics_df['regime_label'].notna()]
            if not regime_label_rows.empty:
                regime_label_str = regime_label_rows.iloc[0].get('regime_label')
                if isinstance(regime_label_str, str) and regime_label_str.strip():
                    regime_map = {'NORMAL': 0, 'CORRECTION': 1, 'CRASH': 2}
                    regime = regime_map.get(regime_label_str.upper())
                if regime is not None:
                    regime_confidence = regime_label_rows.iloc[0].get('regime_confidence', 0.0)
                    if regime_confidence is None or pd.isna(regime_confidence):
                        regime_confidence = 0.0
    
    # Render regime light (handle None case)
    if regime is not None:
        render_regime_light(regime, regime_confidence)
        regime_label = latest_metrics.get('regime_label', 'Unknown')
        if regime_label == 'Unknown' and not metrics_df.empty:
            regime_label = metrics_df.iloc[0].get('regime_label', 'Unknown')
        st.markdown(f"**{regime_label}**")
        if regime_confidence and regime_confidence > 0:
            st.markdown(f"Confidence: {regime_confidence:.1%}")
    else:
        # Show unknown state
        st.markdown("""
        <div class="regime-light" style="background-color: #888888;">
            <div style="text-align: center;">
                <div style="font-size: 0.8rem; margin-bottom: 5px;">UNKNOWN</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("**No regime data**")

with col2:
    vpin_score = latest_metrics.get('vpin', 0.0)
    render_metric_box("VPIN", vpin_score, "", "#FF6600")
    if vpin_score > 0.8:
        st.warning("⚠️ High VPIN Alert!")

with col3:
    vol_score = latest_metrics.get('volatility', 0.0)
    render_metric_box("Volatility", f"{vol_score:.4f}", "", "#FFFF00")

with col4:
    # Calculate average sentiment
    if not sentiment_df.empty and 'sentiment_score' in sentiment_df.columns:
        avg_sentiment = sentiment_df['sentiment_score'].mean()
    else:
        avg_sentiment = 0.0
    sentiment_color = "#00FF00" if avg_sentiment > 0 else "#FF0000" if avg_sentiment < 0 else "#888888"
    render_metric_box("Avg Sentiment", f"{avg_sentiment:+.3f}", "", sentiment_color)

with col5:
    # Data point count
    data_points = len(metrics_df) if not metrics_df.empty else 0
    render_metric_box("Data Points", data_points, "", "#FFFFFF")

st.markdown("---")

# Main Charts Row
col1, col2 = st.columns(2)

with col1:
    st.subheader("Price Chart with VPIN")
    # Try market_df first, then metrics_df if it has OHLC
    chart_df = market_df if not market_df.empty else pd.DataFrame()
    if chart_df.empty and not metrics_df.empty:
        # Check if metrics_df has OHLC columns (API might return them)
        if all(col in metrics_df.columns for col in ['open', 'high', 'low', 'close']):
            chart_df = metrics_df.copy()
    
    if not chart_df.empty:
        # Ensure required columns exist
        if all(col in chart_df.columns for col in ['timestamp', 'open', 'high', 'low', 'close']):
            fig = create_candlestick_chart(chart_df, vpin_overlay=True, title=f"{selected_ticker} Price & VPIN")
            st.plotly_chart(fig, width='stretch')
        else:
            missing = [col for col in ['timestamp', 'open', 'high', 'low', 'close'] if col not in chart_df.columns]
            st.warning(f"Missing columns for price chart: {missing}")
    else:
        st.info("No market data available for this ticker")

with col2:
    st.subheader("Risk Metrics Comparison")
    if not metrics_df.empty:
        fig = create_metrics_comparison(metrics_df, title=f"{selected_ticker} Risk Metrics")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No metrics data available")

# Second Row: Volatility and Sentiment
col1, col2 = st.columns(2)

with col1:
    st.subheader("Yang-Zhang Volatility")
    # Try market_df first, then metrics_df
    vol_df = market_df if not market_df.empty else metrics_df
    if not vol_df.empty and ('vol' in vol_df.columns or 'volatility' in vol_df.columns):
        fig = create_volatility_chart(vol_df, title=f"{selected_ticker} Volatility")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No volatility data available")

with col2:
    st.subheader("Sentiment Timeline")
    if not sentiment_df.empty:
        fig = create_sentiment_timeline(sentiment_df, title=f"{selected_ticker} News Sentiment")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No sentiment data available")

st.markdown("---")

# Plots Viewer Section
st.subheader("📊 Generated Plots")

# Helper function to extract ticker from plot HTML content (for old plots)
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_ticker_from_plot_html(plot_path):
    """Extract ticker from plot HTML by reading the title."""
    try:
        with open(plot_path, 'r', encoding='utf-8') as f:
            content = f.read(10000)  # Read first 10KB (title is usually near the start)
            # Look for ticker in common patterns in HTML
            import re
            ticker_patterns = ['NVDA', 'TSLA', 'AMD', 'SPY', 'AAPL', 'MSFT', 'GOOGL']
            content_upper = content.upper()
            for ticker in ticker_patterns:
                # Look for ticker followed by common words like "Dashboard", "Master", etc.
                if re.search(rf'\b{ticker}\b', content_upper):
                    return ticker
    except:
        pass
    return None

plots_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plots")
if os.path.exists(plots_dir):
    all_plot_files = [f for f in os.listdir(plots_dir) if f.endswith('.html')]
    # No plots found is normal if plots haven't been generated yet
    
    # Filter plots by selected ticker
    ticker_plots = []
    generic_plots = []
    
    for plot_file in all_plot_files:
        plot_path = os.path.join(plots_dir, plot_file)
        
        # Check if filename has ticker prefix (new format)
        if plot_file.startswith(f"{selected_ticker}_"):
            ticker_plots.append(plot_file)
        else:
            # For old format (no prefix), check if it's a generic plot
            # Generic plots start with numbers (1_, 2_, 3_, 4_, 5_)
            if plot_file.startswith(('1_', '2_', '3_', '4_', '5_')):
                generic_plots.append(plot_file)
            else:
                # Check HTML content for ticker match
                plot_ticker = get_ticker_from_plot_html(plot_path)
                if plot_ticker and plot_ticker.upper() == selected_ticker.upper():
                    ticker_plots.append(plot_file)
    
    # Prefer ticker-specific plots, but fall back to generic plots if they exist
    if ticker_plots:
        plot_files = ticker_plots
    elif generic_plots:
        # Show generic plots if no ticker-specific ones exist
        plot_files = sorted(generic_plots)
        st.success(f"✅ Found {len(plot_files)} generic plot(s). These will work for any ticker.")
        st.info(f"💡 Click 'Generate Plots' below to create ticker-specific plots for **{selected_ticker}**.")
    else:
        # No plots found at all
        plot_files = []
        if all_plot_files:
            # There are plots but they don't match our pattern
            st.warning(f"⚠️ Found {len(all_plot_files)} plot file(s) but none match the expected format.")
            st.info(f"Available files: {', '.join(all_plot_files[:5])}{'...' if len(all_plot_files) > 5 else ''}")
        else:
            st.warning(f"⚠️ No plots found. Click 'Generate Plots' to create plots for **{selected_ticker}**.")
    
    if plot_files:
        plot_files.sort()  # Sort alphabetically
        
        # Add generate plots button
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_plot = st.selectbox(
                "Select a plot to view",
                options=plot_files,
                index=0,
                help=f"Choose from generated plots for {selected_ticker}",
                key="plot_selector"
            )
        with col2:
            if st.button("🔄 Generate Plots", use_container_width=True):
                if api_online:
                    with st.spinner(f"Generating plots for {selected_ticker}... This may take 1-2 minutes."):
                        try:
                            import requests
                            response = requests.post(
                                f"{api_client.base_url}/api/plots/generate",
                                json={"ticker": selected_ticker},
                                timeout=180  # 3 minute timeout for plot generation
                            )
                            if response.status_code == 200:
                                st.success(f"✅ Plots generated for {selected_ticker}!")
                                st.rerun()
                            else:
                                st.error(f"Failed to generate plots: {response.text}")
                        except Exception as e:
                            st.error(f"Error generating plots: {str(e)}")
                else:
                    st.error("API is offline. Cannot generate plots.")
        
        if selected_plot:
            plot_path = os.path.join(plots_dir, selected_plot)
            try:
                with open(plot_path, 'r', encoding='utf-8') as f:
                    plot_html = f.read()
                components.html(plot_html, height=600, scrolling=True)
            except Exception as e:
                st.error(f"Error loading plot: {str(e)}")
    else:
        # Show generate button even when no plots exist
        if st.button(f"🔄 Generate Plots for {selected_ticker}", use_container_width=True, key="generate_plots_no_plots"):
            if api_online:
                with st.spinner(f"Generating plots for {selected_ticker}... This may take 1-2 minutes."):
                    try:
                        import requests
                        response = requests.post(
                            f"{api_client.base_url}/api/plots/generate",
                            json={"ticker": selected_ticker},
                            timeout=180
                        )
                        if response.status_code == 200:
                            st.success(f"✅ Plots generated for {selected_ticker}!")
                            st.cache_data.clear()  # Clear cache to reload plot list
                            st.rerun()
                        else:
                            st.error(f"Failed to generate plots: {response.text}")
                    except Exception as e:
                        st.error(f"Error generating plots: {str(e)}")
            else:
                st.error("API is offline. Cannot generate plots.")
else:
    st.info("Plots directory not found")

st.markdown("---")

# News Feed
st.subheader("📰 News Feed")
if not sentiment_df.empty:
    render_news_feed(sentiment_df, max_items=10)
else:
    st.info("No news articles available")

st.markdown("---")

# Bottom: Data Tables (Collapsible)
with st.expander("📋 Raw Metrics Data", expanded=False):
    if not metrics_df.empty:
        # Remove duplicate columns (VPIN/vpin and vol/volatility)
        display_df = metrics_df.copy()
        
        # Drop duplicate columns (keep lowercase versions)
        if 'VPIN' in display_df.columns and 'vpin' in display_df.columns:
            display_df = display_df.drop(columns=['VPIN'])
        if 'volatility' in display_df.columns and 'vol' in display_df.columns:
            display_df = display_df.drop(columns=['vol'])
        
        # Reorder columns for better display
        preferred_order = ['timestamp', 'ticker', 'vpin', 'volatility', 'regime', 'regime_label', 'regime_confidence']
        remaining_cols = [col for col in display_df.columns if col not in preferred_order]
        column_order = [col for col in preferred_order if col in display_df.columns] + remaining_cols
        display_df = display_df[column_order]
        
        st.dataframe(display_df, width='stretch')
    else:
        st.info("No metrics data available")

with st.expander("📋 Market Data", expanded=False):
    if not market_df.empty:
        # Show relevant columns
        display_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        if 'VPIN' in market_df.columns:
            display_cols.append('VPIN')
        if 'vol' in market_df.columns:
            display_cols.append('vol')
        if 'regime' in market_df.columns:
            display_cols.append('regime')
        
        available_cols = [col for col in display_cols if col in market_df.columns]
        st.dataframe(market_df[available_cols].tail(100), width='stretch')
    else:
        st.info("No market data available")

# Auto refresh using Streamlit's built-in refresh
if st.session_state.auto_refresh:
    # Use Streamlit's automatic rerun with interval
    time.sleep(st.session_state.refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #888888; font-size: 0.8rem;'>
        RiskBeacon Dashboard | Bloomberg Terminal Theme | FastAPI Backend
    </div>
    """,
    unsafe_allow_html=True
)

