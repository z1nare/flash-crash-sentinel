# Yet to be implemented.

import talib
# Technical Analysis Library for technical indicators such as:
# Moving Averages (SMA, EMA, WMA, RSI, MACD, Bollinger Bands, etc.)
# Volume-based Indicators (VWAP, OBV, ATR, etc.)
# Momentum Indicators (ROC, MFI, CCI, etc.)
# Overlap Indicators (MACD, RSI, etc.)
# Trend indicators (ADX, ADXW, ATR, etc.) 
import bt # Backtesting library for backtesting trading strategies. 
# Trading signals 
# 1. SMA, EMA signals 
# 2. Trend following strategies :
#    2.1 Bet the price will continue in the same direction as the trend. 
#        Buy when price crosses above SMA/EMA and sell when price crosses below SMA/EMA.  
#        Use ADX to confirm the trend. 
#    2.2 Mean reversion strategies : 
#        Bet the price will revert to the mean.  
#        Use RSI, BBands to confirm the mean reversion. 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 

