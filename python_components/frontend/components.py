"""
Dashboard Components for Streamlit
Regime light, metrics displays, news feed, etc.
"""
import streamlit as st
import pandas as pd
from typing import Optional
from frontend.bloomberg_theme import get_regime_color, get_regime_label

def render_regime_light(regime: Optional[int], confidence: Optional[float] = None):
    """Render regime traffic light widget"""
    if regime is None:
        # Show unknown state
        st.markdown("""
        <div class="regime-light" style="background-color: #888888;">
            <div style="text-align: center;">
                <div style="font-size: 0.8rem; margin-bottom: 5px;">UNKNOWN</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return
    
    color = get_regime_color(regime)
    label = get_regime_label(regime)
    
    # Determine regime class
    if regime == 0:
        regime_class = "normal"
    elif regime == 1:
        regime_class = "correction"
    elif regime == 2:
        regime_class = "crash"
    else:
        regime_class = ""
    
    st.markdown(f"""
    <div class="regime-light regime-{regime_class}" style="background-color: {color};">
        <div style="text-align: center;">
            <div style="font-size: 0.8rem; margin-bottom: 5px;">{label}</div>
            {f'<div style="font-size: 0.7rem;">{confidence:.1%}</div>' if confidence and confidence > 0 else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_metric_box(label: str, value: any, unit: str = "", color: str = "#FF6600"):
    """Render a Bloomberg-style metric box"""
    if isinstance(value, float):
        value_str = f"{value:.4f}"
    elif isinstance(value, int):
        value_str = f"{value:,}"
    else:
        value_str = str(value)
    
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color: {color};">{value_str}{unit}</div>
    </div>
    """, unsafe_allow_html=True)

def render_news_feed(news_df: pd.DataFrame, max_items: int = 10):
    """Render news feed with sentiment badges"""
    if news_df.empty:
        st.info("No news articles available")
        return
    
    # Filter for valid rows
    if 'headline' not in news_df.columns or 'sentiment_label' not in news_df.columns:
        st.warning("News data missing required columns")
        return
    
    news_df = news_df.dropna(subset=['headline'])
    news_df = news_df.head(max_items)
    
    for idx, row in news_df.iterrows():
        headline = str(row.get('headline', 'N/A'))
        ticker = str(row.get('ticker', 'N/A'))
        sentiment_label = str(row.get('sentiment_label', 'neutral'))
        sentiment_score = row.get('sentiment_score', 0.0)
        timestamp = row.get('timestamp', '')
        
        # Determine sentiment class
        sentiment_class = "news-item"
        if 'positive' in sentiment_label.lower():
            sentiment_class += " news-item-positive"
        elif 'negative' in sentiment_label.lower():
            sentiment_class += " news-item-negative"
        
        # Format timestamp
        if pd.notna(timestamp):
            try:
                if isinstance(timestamp, str):
                    ts_str = timestamp[:16]  # Truncate to date and time
                else:
                    ts_str = str(timestamp)[:16]
            except:
                ts_str = ""
        else:
            ts_str = ""
        
        st.markdown(f"""
        <div class="{sentiment_class}">
            <div style="font-weight: bold; color: #FF6600; margin-bottom: 5px;">
                [{ticker}] {headline[:80]}{'...' if len(headline) > 80 else ''}
            </div>
            <div style="font-size: 0.85rem; color: #888888;">
                {ts_str} | Sentiment: {sentiment_label} ({sentiment_score:+.2f})
            </div>
        </div>
        """, unsafe_allow_html=True)

def render_status_indicator(online: bool):
    """Render API connection status indicator"""
    if online:
        st.markdown('<p class="status-online">● API Connected</p>', unsafe_allow_html=True)
    else:
        st.markdown('<p class="status-offline">● API Offline</p>', unsafe_allow_html=True)
        st.error("⚠️ Cannot connect to FastAPI backend. Make sure API is running on http://localhost:8000")

