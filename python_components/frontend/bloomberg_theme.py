"""
Bloomberg Terminal Theme for Streamlit
Black and Orange color scheme
"""
from typing import Optional

BBG_THEME = """
<style>
    /* Bloomberg Terminal Theme */
    :root {
        --bbg-black: #0d1117;
        --bbg-dark-gray: #161b22;
        --bbg-gray: #21262d;
        --bbg-orange: #FF6600;
        --bbg-orange-light: #FF8533;
        --bbg-orange-dark: #CC5200;
        --bbg-green: #00FF00;
        --bbg-red: #FF0000;
        --bbg-yellow: #FFFF00;
        --bbg-white: #FFFFFF;
        --bbg-text: #CCCCCC;
        --bbg-text-dim: #888888;
    }
    
    /* Main App Background */
    .stApp {
        background-color: var(--bbg-black);
        color: var(--bbg-text);
        font-family: 'Courier New', 'Consolas', monospace;
    }
    
    /* Header */
    .stHeader {
        background-color: var(--bbg-dark-gray);
        border-bottom: 2px solid var(--bbg-orange);
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: var(--bbg-dark-gray);
        border-right: 1px solid var(--bbg-orange);
    }
    
    /* Metrics/Value Boxes */
    [data-testid="stMetricValue"] {
        color: var(--bbg-orange);
        font-size: 2rem;
        font-weight: bold;
        font-family: 'Courier New', monospace;
    }
    
    [data-testid="stMetricLabel"] {
        color: var(--bbg-text);
        font-size: 0.9rem;
        font-family: 'Courier New', monospace;
    }
    
    /* Buttons */
    .stButton > button {
        background-color: var(--bbg-orange);
        color: var(--bbg-black);
        border: 1px solid var(--bbg-orange);
        border-radius: 4px;
        font-weight: bold;
        font-family: 'Courier New', monospace;
        transition: all 0.3s;
    }
    
    .stButton > button:hover {
        background-color: var(--bbg-orange-light);
        border-color: var(--bbg-orange-light);
    }
    
    /* Selectbox/Dropdown */
    .stSelectbox > div > div {
        background-color: var(--bbg-gray);
        color: var(--bbg-text);
        border: 1px solid var(--bbg-orange);
    }
    
    /* Text Input */
    .stTextInput > div > div > input {
        background-color: var(--bbg-gray);
        color: var(--bbg-text);
        border: 1px solid var(--bbg-orange);
    }
    
    /* Slider */
    .stSlider > div > div {
        background-color: var(--bbg-gray);
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: var(--bbg-dark-gray);
        color: var(--bbg-text);
        border: 1px solid var(--bbg-orange);
    }
    
    /* Dataframe */
    .stDataFrame {
        background-color: var(--bbg-dark-gray);
    }
    
    /* Custom Regime Light */
    .regime-light {
        width: 120px;
        height: 120px;
        border-radius: 50%;
        margin: 20px auto;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: bold;
        font-size: 0.9rem;
        color: var(--bbg-black);
        box-shadow: 0 0 30px currentColor;
        transition: all 0.3s;
    }
    
    .regime-normal {
        background-color: var(--bbg-green);
        box-shadow: 0 0 30px var(--bbg-green);
    }
    
    .regime-correction {
        background-color: var(--bbg-yellow);
        box-shadow: 0 0 30px var(--bbg-yellow);
    }
    
    .regime-crash {
        background-color: var(--bbg-red);
        box-shadow: 0 0 30px var(--bbg-red);
        animation: pulse 1s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    /* Metric Container */
    .metric-container {
        background-color: var(--bbg-dark-gray);
        border: 2px solid var(--bbg-orange);
        border-radius: 6px;
        padding: 15px;
        margin: 10px 0;
    }
    
    .metric-value {
        color: var(--bbg-orange);
        font-size: 2.5rem;
        font-weight: bold;
        font-family: 'Courier New', monospace;
        margin: 0;
    }
    
    .metric-label {
        color: var(--bbg-text);
        font-size: 1rem;
        font-family: 'Courier New', monospace;
        margin-bottom: 5px;
    }
    
    /* News Feed Item */
    .news-item {
        background-color: var(--bbg-dark-gray);
        border-left: 4px solid var(--bbg-orange);
        padding: 10px;
        margin: 5px 0;
        border-radius: 4px;
    }
    
    .news-item-positive {
        border-left-color: var(--bbg-green);
    }
    
    .news-item-negative {
        border-left-color: var(--bbg-red);
    }
    
    /* Status Badge */
    .status-online {
        color: var(--bbg-green);
        font-weight: bold;
    }
    
    .status-offline {
        color: var(--bbg-red);
        font-weight: bold;
    }
    
    /* Title Styling */
    h1 {
        color: var(--bbg-orange);
        font-family: 'Courier New', monospace;
        border-bottom: 2px solid var(--bbg-orange);
        padding-bottom: 10px;
    }
    
    h2, h3 {
        color: var(--bbg-orange-light);
        font-family: 'Courier New', monospace;
    }
    
    /* Code blocks */
    code {
        background-color: var(--bbg-dark-gray);
        color: var(--bbg-orange);
        padding: 2px 6px;
        border-radius: 3px;
    }
</style>
"""

def apply_theme():
    """Apply Bloomberg theme to Streamlit app"""
    import streamlit as st
    st.markdown(BBG_THEME, unsafe_allow_html=True)

def get_regime_color(regime: Optional[int]) -> str:
    """Get color for regime state"""
    if regime == 0:
        return "#00FF00"  # Green - Normal
    elif regime == 1:
        return "#FFFF00"  # Yellow - Correction
    elif regime == 2:
        return "#FF0000"  # Red - Crash
    else:
        return "#888888"  # Gray - Unknown

def get_regime_label(regime: Optional[int]) -> str:
    """Get label for regime state"""
    labels = {
        0: "NORMAL",
        1: "CORRECTION",
        2: "CRASH"
    }
    return labels.get(regime, "UNKNOWN")

