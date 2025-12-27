"""
API Routes for RiskBeacon
"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from datetime import datetime
from typing import List, Optional
import pandas as pd
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from controllers.ServiceController import ServiceController
from backend.models.domain import TickerDTO, NewsDTO
from services.plotService import generate_all_plots, DEFAULT_OUTPUT_DIR
from api.schemas import (
    TickRequest, TickResponse,
    NewsRequest, NewsResponse,
    MetricsHistoryResponse,
    StatusResponse,
    PlotGenerateRequest, PlotGenerateResponse,
    SentimentAnalyzeRequest, SentimentAnalyzeResponse
)

router = APIRouter(prefix="/api", tags=["sentinel"])

# This will be set by main.py
_controller_instance: ServiceController = None

def set_controller(controller: ServiceController):
    """Set the controller instance (called from main.py)"""
    global _controller_instance
    _controller_instance = controller

def get_controller() -> ServiceController:
    """Dependency injection for ServiceController"""
    if _controller_instance is None:
        raise HTTPException(status_code=500, detail="ServiceController not initialized")
    return _controller_instance

@router.post("/tick", response_model=TickResponse)
async def process_tick(
    request: TickRequest,
    controller: ServiceController = Depends(get_controller)
):
    """
    Process a new market tick (OHLC data).
    Triggers VPIN and Volatility calculation if bucket is full.
    """
    try:
        # Validate request data
        if not request.ticker or not request.ticker.strip():
            raise HTTPException(status_code=400, detail="Ticker is required")
        
        if request.volume < 0:
            raise HTTPException(status_code=400, detail="Volume must be non-negative")
        
        if request.high < request.low:
            raise HTTPException(status_code=400, detail="High must be >= Low")
        
        if not (request.low <= request.open <= request.high):
            raise HTTPException(status_code=400, detail="Open must be between Low and High")
        
        if not (request.low <= request.close <= request.high):
            raise HTTPException(status_code=400, detail="Close must be between Low and High")
        
        # Convert request to TickerDTO
        ticker_dto = TickerDTO(
            event_type=request.event_type or "TICK",
            timestamp=request.timestamp,
            ticker=request.ticker.strip().upper(),
            open=float(request.open),
            high=float(request.high),
            low=float(request.low),
            close=float(request.close),
            volume=int(request.volume)
        )
        
        # Process through controller
        controller.process_tick(ticker_dto)
        
        # Check if metrics were generated (VPIN bucket filled)
        vpin_score = None
        vol_score = None
        ticker_upper = ticker_dto.ticker.upper()
        
        try:
            # Get VPIN score from state
            if hasattr(controller.vpin_service, 'state') and ticker_upper in controller.vpin_service.state:
                state = controller.vpin_service.state[ticker_upper]
                vpin_score = state.last_vpin if hasattr(state, 'last_vpin') and state.last_vpin > 0 else None
            
            # Try to get volatility and regime from the CSV (if it was just calculated)
            # Read the last row from the ticker's CSV to get volatility and regime
            csv_path = controller.get_ticker_csv_path(ticker_upper)
            regime_state, regime_label, regime_confidence = None, None, None
            
            if os.path.exists(csv_path):
                try:
                    # Read CSV with error handling for malformed rows
                    try:
                        df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='skip')
                    except TypeError:
                        try:
                            df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                        except TypeError:
                            df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False)
                    if not df.empty:
                        # Get the last row's values (most recently calculated)
                        if 'vol' in df.columns:
                            last_vol = df['vol'].iloc[-1]
                            if pd.notna(last_vol) and float(last_vol) > 0:
                                vol_score = float(last_vol)
                        
                        # Get regime information if available
                        if 'regime' in df.columns:
                            last_regime = df['regime'].iloc[-1]
                            if pd.notna(last_regime):
                                regime_state = int(last_regime)
                        # Get regime label from regime service
                        regime_service = controller.get_regime_service(ticker_upper)
                        if regime_service:
                            regime_label = regime_service.get_regime_label(regime_state)
                        else:
                            # Fallback to basic labels
                            regime_labels = {0: "Low Vol / Normal", 1: "High Vol / Correction", 2: "Crash / Liquidity Crisis"}
                            regime_label = regime_labels.get(regime_state, f"Unknown ({regime_state})")
                        
                        if 'regime_confidence' in df.columns:
                            last_confidence = df['regime_confidence'].iloc[-1]
                            if pd.notna(last_confidence):
                                regime_confidence = float(last_confidence)
                except Exception as e:
                    print(f"[API] Error reading CSV for regime: {e}")
                    pass
        except Exception:
            # If state access fails, continue without scores
            pass
        
        return TickResponse(
            success=True,
            message=f"Tick processed for {ticker_dto.ticker}",
            vpin_calculated=vpin_score is not None and vpin_score > 0,
            vpin_score=float(vpin_score) if vpin_score is not None else 0.0,
            volatility_calculated=vol_score is not None and vol_score > 0,
            volatility_score=float(vol_score) if vol_score is not None else 0.0,
            regime=regime_state,
            regime_label=regime_label,
            regime_confidence=regime_confidence
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input data: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing tick: {str(e)}")

@router.post("/news", response_model=NewsResponse)
async def process_news(
    request: NewsRequest,
    controller: ServiceController = Depends(get_controller)
):
    """
    Process a news headline.
    Triggers sentiment analysis and persists result.
    """
    try:
        # Validate request data
        if not request.ticker or not request.ticker.strip():
            raise HTTPException(status_code=400, detail="Ticker is required")
        
        if not request.headline or not request.headline.strip():
            raise HTTPException(status_code=400, detail="Headline is required")
        
        if not request.url or not request.url.strip():
            raise HTTPException(status_code=400, detail="URL is required")
        
        # Convert request to NewsDTO
        news_dto = NewsDTO(
            event_type=request.event_type or "NEWS",
            timestamp=request.timestamp,
            ticker=request.ticker.strip().upper(),
            headline=request.headline.strip(),
            url=request.url.strip()
        )
        
        # Process through controller (this returns the sentiment DTO)
        sentiment_dto = controller.sentiment_service.process_news(news_dto)
        
        # Persist the sentiment result
        controller._persist_article_sentiment(news_dto, sentiment_dto)
        
        return NewsResponse(
            success=True,
            message=f"News processed for {news_dto.ticker}",
            ticker=news_dto.ticker,
            sentiment_score=float(sentiment_dto.sentiment_score),
            sentiment_label=str(sentiment_dto.sentiment_label)
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input data: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing news: {str(e)}")

@router.post("/sentiment/analyze", response_model=SentimentAnalyzeResponse)
async def analyze_sentiment(
    request: SentimentAnalyzeRequest,
    controller: ServiceController = Depends(get_controller)
):
    """
    Analyze sentiment of a news article or headline text.
    Returns sentiment score and label without persisting to database.
    
    This is a simple endpoint for quick sentiment analysis without the full news processing pipeline.
    """
    try:
        # Validate input
        if not request.text or not request.text.strip():
            raise HTTPException(status_code=400, detail="Text is required")
        
        text = request.text.strip()
        
        # Truncate text if too long for display (FinBERT handles this internally)
        display_text = text[:200] + "..." if len(text) > 200 else text
        
        # Create a temporary NewsDTO for processing (ticker and timestamp not needed for analysis)
        from datetime import datetime
        temp_news_dto = NewsDTO(
            event_type="NEWS",
            timestamp=datetime.now(),
            ticker="TEMP",  # Not used for sentiment analysis
            headline=text,
            url=""  # Not needed for sentiment analysis
        )
        
        # Process sentiment using the sentiment service
        sentiment_dto = controller.sentiment_service.process_news(temp_news_dto)
        
        return SentimentAnalyzeResponse(
            success=True,
            sentiment_score=sentiment_dto.sentiment_score,
            sentiment_label=sentiment_dto.sentiment_label,
            text=display_text
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input data: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing sentiment: {str(e)}")

@router.get("/metrics/history", response_model=List[MetricsHistoryResponse])
async def get_metrics_history(
    ticker: Optional[str] = None,
    limit: int = 100,
    controller: ServiceController = Depends(get_controller)
):
    """
    Get historical metrics (VPIN, Volatility) from ticker-specific CSV files.
    Requires ticker parameter - reads from historicalData/{ticker}.csv
    """
    # Ticker is now required
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker parameter is required. Example: /api/metrics/history?ticker=NVDA")
    
    ticker = ticker.upper().strip()
    
    # Validate limit parameter
    if limit < 1:
        limit = 1
    elif limit > 10000:
        limit = 10000  # Cap at reasonable maximum
    try:
        # Get ticker-specific CSV path
        csv_path = controller.get_ticker_csv_path(ticker)
        
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail=f"No data found for ticker '{ticker}'. File not found: {csv_path}")
        
        # Read CSV with error handling for malformed rows
        try:
            try:
                df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='skip')
            except TypeError:
                # Older pandas versions - use error_bad_lines instead
                try:
                    df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                except TypeError:
                    # Even older versions
                    df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read CSV: {str(e)}")
        
        if df.empty:
            return []
        
        # Use existing column names: VPIN (uppercase) and vol (lowercase)
        if 'VPIN' not in df.columns:
            df['VPIN'] = None
        if 'vol' not in df.columns:
            df['vol'] = None
        if 'regime' not in df.columns:
            df['regime'] = None
        if 'regime_confidence' not in df.columns:
            df['regime_confidence'] = None
        
        # Filter out rows where VPIN or vol are NaN or empty (both)
        df = df.dropna(subset=['VPIN', 'vol'], how='all')
        
        # All rows should be for this ticker (since it's a ticker-specific file)
        # But filter just in case
        if 'ticker' in df.columns:
            df = df[df['ticker'] == ticker]
        
        if df.empty:
            return []
        
        # Parse timestamp with robust error handling (normalize timezone)
        def safe_parse_timestamp(ts):
            """Safely parse timestamp, return None if invalid"""
            if pd.isna(ts):
                return None
            try:
                # Try parsing as datetime
                if isinstance(ts, str):
                    parsed = pd.to_datetime(ts, errors='coerce')
                elif isinstance(ts, (int, float)):
                    # Skip numeric values that aren't timestamps
                    return None
                else:
                    parsed = pd.to_datetime(ts, errors='coerce')
                
                if pd.isna(parsed):
                    return None
                
                # Normalize to timezone-naive to avoid comparison issues
                if hasattr(parsed, 'tz') and parsed.tz is not None:
                    parsed = parsed.tz_convert(None)
                
                return parsed
            except:
                return None
        
        # Apply safe timestamp parsing
        df['timestamp'] = df['timestamp'].apply(safe_parse_timestamp)
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=['timestamp'])
        
        if df.empty:
            return []
        
        # Sort by timestamp
        df = df.sort_values('timestamp', ascending=False)
        
        # Limit results
        df = df.head(limit)
        
        # Convert to response format with validation
        results = []
        for _, row in df.iterrows():
            try:
                # Ensure numeric values are valid - use VPIN and vol columns
                vpin_val = float(row.get('VPIN', 0.0)) if pd.notna(row.get('VPIN')) else 0.0
                vol_val = float(row.get('vol', 0.0)) if pd.notna(row.get('vol')) else 0.0
                ticker_val = str(row.get('ticker', 'UNKNOWN'))
                ts_val = row.get('timestamp')
                
                # Get regime information
                regime_val = None
                regime_label_val = None
                regime_confidence_val = None
                
                if pd.notna(row.get('regime')):
                    try:
                        regime_val = int(row.get('regime'))
                        # Get regime label from regime service
                        regime_service = controller.get_regime_service(ticker)
                        if regime_service:
                            regime_label_val = regime_service.get_regime_label(regime_val)
                        else:
                            # Fallback to basic labels
                            regime_labels = {0: "Low Vol / Normal", 1: "High Vol / Correction", 2: "Crash / Liquidity Crisis"}
                            regime_label_val = regime_labels.get(regime_val, f"Unknown ({regime_val})")
                    except (ValueError, TypeError):
                        pass
                
                if pd.notna(row.get('regime_confidence')):
                    try:
                        regime_confidence_val = float(row.get('regime_confidence'))
                    except (ValueError, TypeError):
                        pass
                
                if ts_val is None or pd.isna(ts_val):
                    continue
                
                # Skip rows where both metrics are zero/NaN (no real data)
                if vpin_val == 0.0 and vol_val == 0.0:
                    continue
                
                results.append(MetricsHistoryResponse(
                    timestamp=ts_val if isinstance(ts_val, datetime) else pd.to_datetime(ts_val),
                    ticker=ticker_val,
                    vpin=vpin_val,
                    volatility=vol_val,
                    regime=regime_val,
                    regime_label=regime_label_val,
                    regime_confidence=regime_confidence_val
                ))
            except Exception as e:
                # Skip invalid rows
                continue
        
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving metrics history: {str(e)}")

@router.get("/tickers")
async def list_tickers(controller: ServiceController = Depends(get_controller)):
    """
    List all available tickers with data files in historicalData/.
    """
    try:
        if not os.path.exists(controller.HISTORICAL_DATA_DIR):
            return {"tickers": [], "message": "historicalData directory not found"}
        
        ticker_files = [f.replace('.csv', '').upper() 
                       for f in os.listdir(controller.HISTORICAL_DATA_DIR) 
                       if f.endswith('.csv') and os.path.isfile(os.path.join(controller.HISTORICAL_DATA_DIR, f))]
        
        ticker_files.sort()
        
        return {
            "tickers": ticker_files,
            "count": len(ticker_files),
            "data_directory": controller.HISTORICAL_DATA_DIR
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing tickers: {str(e)}")

@router.get("/status", response_model=StatusResponse)
async def get_status(
    controller: ServiceController = Depends(get_controller)
):
    """
    Get current status of all services and recent metrics across all tickers.
    Reads from all ticker-specific CSV files in historicalData/.
    """
    try:
        latest_metrics = {}
        
        # Get all ticker CSV files from historicalData directory
        if os.path.exists(controller.HISTORICAL_DATA_DIR):
            ticker_files = [f for f in os.listdir(controller.HISTORICAL_DATA_DIR) 
                          if f.endswith('.csv') and os.path.isfile(os.path.join(controller.HISTORICAL_DATA_DIR, f))]
            
            for ticker_file in ticker_files:
                ticker_name = ticker_file.replace('.csv', '').upper()
                csv_path = os.path.join(controller.HISTORICAL_DATA_DIR, ticker_file)
                
                try:
                    # Read CSV with error handling for malformed rows
                    try:
                        df = pd.read_csv(csv_path, low_memory=False, on_bad_lines='skip')
                    except TypeError:
                        try:
                            df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False, warn_bad_lines=True)
                        except TypeError:
                            df = pd.read_csv(csv_path, low_memory=False, error_bad_lines=False)
                    
                    if not df.empty and 'timestamp' in df.columns:
                        # Check for metrics columns
                        has_vpin = 'VPIN' in df.columns
                        has_vol = 'vol' in df.columns
                        
                        if has_vpin and has_vol:
                            # Filter for rows with valid metrics (non-zero, non-NaN)
                            df = df.dropna(subset=['VPIN', 'vol'], how='all')
                            # Also filter out rows where both are zero
                            df = df[~((df['VPIN'].fillna(0) == 0) & (df['vol'].fillna(0) == 0))]
                            
                            if not df.empty:
                                # Safe timestamp parsing (normalize timezone)
                                def safe_parse_timestamp(ts):
                                    if pd.isna(ts):
                                        return None
                                    try:
                                        if isinstance(ts, str):
                                            parsed = pd.to_datetime(ts, errors='coerce')
                                        elif isinstance(ts, (int, float)):
                                            # Skip numeric values
                                            return None
                                        else:
                                            parsed = pd.to_datetime(ts, errors='coerce')
                                        
                                        if pd.isna(parsed):
                                            return None
                                        
                                        # Normalize to timezone-naive to avoid comparison issues
                                        if hasattr(parsed, 'tz') and parsed.tz is not None:
                                            parsed = parsed.tz_convert(None)
                                        
                                        return parsed
                                    except:
                                        return None
                                
                                df['timestamp'] = df['timestamp'].apply(safe_parse_timestamp)
                                df = df.dropna(subset=['timestamp'])
                                
                                if not df.empty:
                                    df = df.sort_values('timestamp', ascending=False)
                                    
                                    # Get latest for this ticker (should only be one ticker per file)
                                    latest_row = df.iloc[0]
                                    try:
                                        # Use VPIN and vol columns
                                        vpin_val = float(latest_row.get('VPIN', 0.0)) if pd.notna(latest_row.get('VPIN')) else 0.0
                                        vol_val = float(latest_row.get('vol', 0.0)) if pd.notna(latest_row.get('vol')) else 0.0
                                        ts_val = latest_row['timestamp']
                                        
                                        # Only include if at least one metric is non-zero
                                        if ts_val is not None and not pd.isna(ts_val) and (vpin_val != 0.0 or vol_val != 0.0):
                                            latest_metrics[ticker_name] = {
                                                "vpin": vpin_val,
                                                "volatility": vol_val,
                                                "timestamp": ts_val.isoformat() if hasattr(ts_val, 'isoformat') else str(ts_val)
                                            }
                                    except Exception:
                                        # Skip invalid rows
                                        pass
                except Exception:
                    # Skip files that can't be read
                    continue
        
        # Get VPIN state info
        vpin_states = {}
        try:
            for ticker, state in controller.vpin_service.state.items():
                vpin_states[str(ticker)] = {
                    "current_bucket_volume": float(state.current_bucket_volume),
                    "last_vpin": float(state.last_vpin),
                    "buckets_in_window": int(len(state.imbalance_history))
                }
        except Exception:
            # If state access fails, continue with empty states
            pass
        
        return StatusResponse(
            services_ready=True,
            latest_metrics=latest_metrics,
            vpin_states=vpin_states
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving status: {str(e)}")

# ========== PLOT ENDPOINTS ==========

@router.post("/plots/generate", response_model=PlotGenerateResponse)
async def generate_plots(
    request: PlotGenerateRequest,
    controller: ServiceController = Depends(get_controller)
):
    """
    Generate all interactive plots from historical data.
    Creates 5 plots: Sentinel Dashboard, Liquidity Heatmap, Volatility Cone, Sentiment Impact, Crash Probability Gauge.
    
    Note: This operation may take 60-180 seconds for large datasets.
    """
    try:
        # Use ticker-specific data path from historicalData/
        if not request.ticker:
            raise HTTPException(status_code=400, detail="Ticker parameter is required for plot generation")
        
        ticker = request.ticker.upper().strip()
        data_path = controller.get_ticker_csv_path(ticker)
        
        if not os.path.exists(data_path):
            raise HTTPException(
                status_code=404, 
                detail=f"No data file found for ticker '{ticker}'. Expected: {data_path}"
            )
        
        # Try to find sentiment file (with_sentiment version)
        sentiment_path = controller.ARTICLES_CSV_PATH.replace('articles.csv', 'articles_with_sentiment.csv')
        if not os.path.exists(sentiment_path):
            # Fallback to original articles path
            sentiment_path = controller.ARTICLES_CSV_PATH if os.path.exists(controller.ARTICLES_CSV_PATH) else None
        
        # Run plot generation in thread pool to avoid blocking (Plotly HTML generation is CPU-bound)
        loop = asyncio.get_event_loop()
        sentiment_path_final = sentiment_path if sentiment_path and os.path.exists(sentiment_path) else None
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            plot_paths = await loop.run_in_executor(
                executor,
                lambda: generate_all_plots(
                    data_path=data_path,
                    sentiment_path=sentiment_path_final,
                    ticker=request.ticker,
                    output_dir=DEFAULT_OUTPUT_DIR
                )
            )
        
        if not plot_paths:
            raise HTTPException(
                status_code=404,
                detail="No plots generated. Check if data files exist and contain valid data."
            )
        
        # Use the provided ticker
        ticker_used = ticker
        
        # Convert absolute paths to relative URLs for API response (faster serialization)
        plots_urls = {}
        for plot_name, plot_path in plot_paths.items():
            # Return relative URL instead of full path
            plots_urls[plot_name] = f"/api/plots/view/{plot_name}"
        
        return PlotGenerateResponse(
            success=True,
            message=f"Successfully generated {len(plot_paths)} plots",
            ticker=ticker_used,
            plots_generated=plots_urls
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating plots: {str(e)}")

@router.get("/plots/list")
async def list_plots():
    """
    List all available plot files.
    """
    try:
        if not os.path.exists(DEFAULT_OUTPUT_DIR):
            return {"plots": [], "message": "No plots directory found. Generate plots first."}
        
        plot_files = [
            "1_sentinel_dashboard.html",
            "2_liquidity_heatmap.html",
            "3_volatility_cone.html",
            "4_sentiment_impact.html",
            "5_crash_gauge.html"
        ]
        
        available_plots = {}
        for plot_file in plot_files:
            plot_path = os.path.join(DEFAULT_OUTPUT_DIR, plot_file)
            if os.path.exists(plot_path):
                available_plots[plot_file] = {
                    "path": plot_path,
                    "url": f"/api/plots/view/{plot_file}",
                    "exists": True
                }
            else:
                available_plots[plot_file] = {
                    "exists": False
                }
        
        return {
            "plots": available_plots,
            "total_available": sum(1 for p in available_plots.values() if p.get("exists", False))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing plots: {str(e)}")

@router.get("/plots/view/{plot_name}", response_class=HTMLResponse)
async def view_plot(plot_name: str):
    """
    Serve a specific plot HTML file.
    """
    try:
        # Security: Only allow specific plot names
        allowed_plots = [
            "1_sentinel_dashboard.html",
            "2_liquidity_heatmap.html",
            "3_volatility_cone.html",
            "4_sentiment_impact.html",
            "5_crash_gauge.html"
        ]
        
        if plot_name not in allowed_plots:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid plot name. Allowed: {', '.join(allowed_plots)}"
            )
        
        plot_path = os.path.join(DEFAULT_OUTPUT_DIR, plot_name)
        
        if not os.path.exists(plot_path):
            raise HTTPException(
                status_code=404,
                detail=f"Plot '{plot_name}' not found. Generate plots first using POST /api/plots/generate"
            )
        
        return FileResponse(plot_path, media_type="text/html")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error serving plot: {str(e)}")

