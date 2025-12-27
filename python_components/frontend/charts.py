"""
Chart Components for Streamlit Dashboard
Using Plotly for interactive charts
"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Optional

# Bloomberg Terminal Theme Colors
BBG_BLACK = "#0d1117"
BBG_DARK_GRAY = "#161b22"
BBG_GRAY = "#21262d"
BBG_ORANGE = "#FF6600"
BBG_ORANGE_LIGHT = "#FF8533"
BBG_GREEN = "#00FF00"
BBG_RED = "#FF0000"
BBG_YELLOW = "#FFFF00"
BBG_WHITE = "#FFFFFF"
BBG_TEXT = "#CCCCCC"

def create_candlestick_chart(
    df: pd.DataFrame,
    vpin_overlay: bool = True,
    title: str = "Price Chart with VPIN"
) -> go.Figure:
    """Create candlestick chart with optional VPIN heatmap overlay"""
    if df.empty or 'timestamp' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    df = df.sort_values('timestamp')
    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close'])
    
    # Create subplots
    if vpin_overlay and 'VPIN' in df.columns:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3],
            subplot_titles=(title, "VPIN Level")
        )
        
        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name="Price",
                increasing_line_color=BBG_GREEN,
                decreasing_line_color=BBG_RED,
            ),
            row=1, col=1
        )
        
        # VPIN overlay
        vpin_data = df['VPIN'].fillna(0)
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=vpin_data,
                mode='lines',
                name='VPIN',
                line=dict(color=BBG_ORANGE, width=2),
                fill='tozeroy',
                fillcolor=f'rgba(255, 102, 0, 0.2)'
            ),
            row=2, col=1
        )
        
        # VPIN threshold line (0.8)
        fig.add_hline(
            y=0.8,
            line_dash="dash",
            line_color=BBG_RED,
            annotation_text="VPIN Alert (0.8)",
            row=2, col=1
        )
    else:
        fig = go.Figure()
        fig.add_trace(
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name="Price",
                increasing_line_color=BBG_GREEN,
                decreasing_line_color=BBG_RED,
            )
        )
        fig.update_layout(title=title)
    
    # Update layout with Bloomberg theme
    fig.update_layout(
        plot_bgcolor=BBG_BLACK,
        paper_bgcolor=BBG_BLACK,
        font=dict(color=BBG_TEXT, family="Courier New"),
        xaxis=dict(gridcolor=BBG_GRAY),
        yaxis=dict(gridcolor=BBG_GRAY),
        xaxis2=dict(gridcolor=BBG_GRAY),
        yaxis2=dict(gridcolor=BBG_GRAY),
        height=600,
        showlegend=True,
        legend=dict(
            bgcolor=BBG_DARK_GRAY,
            bordercolor=BBG_ORANGE,
            borderwidth=1
        )
    )
    
    return fig

def create_volatility_chart(
    df: pd.DataFrame,
    title: str = "Yang-Zhang Volatility"
) -> go.Figure:
    """Create volatility chart"""
    # Check for volatility column (vol or volatility)
    vol_col = None
    if 'vol' in df.columns:
        vol_col = 'vol'
    elif 'volatility' in df.columns:
        vol_col = 'volatility'
    
    if df.empty or 'timestamp' not in df.columns or vol_col is None:
        fig = go.Figure()
        fig.add_annotation(text="No volatility data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    df = df.sort_values('timestamp')
    df = df.dropna(subset=['timestamp', vol_col])
    
    vol_data = df[vol_col].fillna(0)
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=vol_data,
            mode='lines',
            name='Volatility',
            line=dict(color=BBG_ORANGE, width=2),
            fill='tozeroy',
            fillcolor=f'rgba(255, 102, 0, 0.2)'
        )
    )
    
    # Add moving average
    if len(vol_data) > 20:
        vol_ma = vol_data.rolling(window=20, min_periods=1).mean()
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=vol_ma,
                mode='lines',
                name='20-period MA',
                line=dict(color=BBG_YELLOW, width=1, dash='dash')
            )
        )
    
    fig.update_layout(
        title=title,
        plot_bgcolor=BBG_BLACK,
        paper_bgcolor=BBG_BLACK,
        font=dict(color=BBG_TEXT, family="Courier New"),
        xaxis=dict(gridcolor=BBG_GRAY, title="Time"),
        yaxis=dict(gridcolor=BBG_GRAY, title="Volatility"),
        height=400,
        showlegend=True,
        legend=dict(
            bgcolor=BBG_DARK_GRAY,
            bordercolor=BBG_ORANGE,
            borderwidth=1
        )
    )
    
    return fig

def create_sentiment_timeline(
    df: pd.DataFrame,
    title: str = "Sentiment Over Time"
) -> go.Figure:
    """Create sentiment timeline chart"""
    if df.empty or 'timestamp' not in df.columns or 'sentiment_score' not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No sentiment data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    df = df.sort_values('timestamp')
    df = df.dropna(subset=['timestamp', 'sentiment_score'])
    
    # Color based on sentiment
    colors = [BBG_GREEN if x >= 0 else BBG_RED for x in df['sentiment_score']]
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Bar(
            x=df['timestamp'],
            y=df['sentiment_score'],
            name='Sentiment',
            marker_color=colors,
            text=[f"{x:.2f}" for x in df['sentiment_score']],
            textposition='outside'
        )
    )
    
    # Zero line
    fig.add_hline(y=0, line_dash="dash", line_color=BBG_TEXT, line_width=1)
    
    fig.update_layout(
        title=title,
        plot_bgcolor=BBG_BLACK,
        paper_bgcolor=BBG_BLACK,
        font=dict(color=BBG_TEXT, family="Courier New"),
        xaxis=dict(gridcolor=BBG_GRAY, title="Time"),
        yaxis=dict(gridcolor=BBG_GRAY, title="Sentiment Score (-1 to +1)"),
        height=300,
        showlegend=False
    )
    
    return fig

def create_metrics_comparison(
    metrics_df: pd.DataFrame,
    title: str = "Risk Metrics Comparison"
) -> go.Figure:
    """Create comparison chart of VPIN and Volatility"""
    if metrics_df.empty or 'timestamp' not in metrics_df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No metrics data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    metrics_df = metrics_df.sort_values('timestamp')
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("VPIN", "Volatility")
    )
    
    # VPIN
    if 'vpin' in metrics_df.columns or 'VPIN' in metrics_df.columns:
        vpin_col = 'VPIN' if 'VPIN' in metrics_df.columns else 'vpin'
        vpin_data = metrics_df[vpin_col].fillna(0)
        fig.add_trace(
            go.Scatter(
                x=metrics_df['timestamp'],
                y=vpin_data,
                mode='lines',
                name='VPIN',
                line=dict(color=BBG_ORANGE, width=2)
            ),
            row=1, col=1
        )
        fig.add_hline(y=0.8, line_dash="dash", line_color=BBG_RED, annotation_text="Alert", row=1, col=1)
    
    # Volatility
    if 'volatility' in metrics_df.columns or 'vol' in metrics_df.columns:
        vol_col = 'vol' if 'vol' in metrics_df.columns else 'volatility'
        vol_data = metrics_df[vol_col].fillna(0)
        fig.add_trace(
            go.Scatter(
                x=metrics_df['timestamp'],
                y=vol_data,
                mode='lines',
                name='Volatility',
                line=dict(color=BBG_YELLOW, width=2)
            ),
            row=2, col=1
        )
    
    fig.update_layout(
        title=title,
        plot_bgcolor=BBG_BLACK,
        paper_bgcolor=BBG_BLACK,
        font=dict(color=BBG_TEXT, family="Courier New"),
        height=500,
        showlegend=False
    )
    
    fig.update_xaxes(gridcolor=BBG_GRAY, row=1, col=1)
    fig.update_yaxes(gridcolor=BBG_GRAY, row=1, col=1)
    fig.update_xaxes(gridcolor=BBG_GRAY, row=2, col=1)
    fig.update_yaxes(gridcolor=BBG_GRAY, row=2, col=1)
    
    return fig

