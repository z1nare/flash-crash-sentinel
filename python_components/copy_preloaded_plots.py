#!/usr/bin/env python3
"""
Copy preloaded plots from Downloads to plots directory
Renames them to match the expected ticker-specific format
"""
import os
import shutil
from pathlib import Path

# Source directory (Downloads)
DOWNLOADS_DIR = Path.home() / "Downloads"

# Destination directory (plots)
BASE_DIR = Path(__file__).parent
PLOTS_DIR = BASE_DIR / "plots"

# Create plots directory if it doesn't exist
PLOTS_DIR.mkdir(exist_ok=True)

# Map of generic plot names to ticker-specific names
# Format: {generic_name: ticker_specific_prefix}
PLOT_MAPPING = {
    "1_sentinel_dashboard.html": "NVDA_1_sentinel_dashboard.html",
    "2_liquidity_heatmap.html": "NVDA_2_liquidity_heatmap.html",
    "3_volatility_cone.html": "NVDA_3_volatility_cone.html",
    "4_sentiment_impact.html": "NVDA_4_sentiment_impact.html",
    "5_crash_gauge.html": "NVDA_5_crash_gauge.html",
}

# Alternative: if files have different names, map them
ALTERNATIVE_NAMES = {
    "sentinel_dashboard.html": "NVDA_1_sentinel_dashboard.html",
    "liquidity_heatmap.html": "NVDA_2_liquidity_heatmap.html",
    "volatility_cone.html": "NVDA_3_volatility_cone.html",
    "sentiment_impact.html": "NVDA_4_sentiment_impact.html",
    "crash_gauge.html": "NVDA_5_crash_gauge.html",
}

def find_plot_files():
    """Find plot files in Downloads directory"""
    found_files = {}
    
    # Check for exact matches first
    for generic_name, ticker_name in PLOT_MAPPING.items():
        source_path = DOWNLOADS_DIR / generic_name
        if source_path.exists():
            found_files[ticker_name] = source_path
            print(f"✅ Found: {generic_name}")
    
    # Check for alternative names
    for alt_name, ticker_name in ALTERNATIVE_NAMES.items():
        if ticker_name not in found_files:  # Don't overwrite if already found
            source_path = DOWNLOADS_DIR / alt_name
            if source_path.exists():
                found_files[ticker_name] = source_path
                print(f"✅ Found (alternative name): {alt_name}")
    
    # Also check for files starting with numbers
    for file in DOWNLOADS_DIR.glob("*.html"):
        filename = file.name
        if filename.startswith(("1_", "2_", "3_", "4_", "5_")):
            # Determine which plot number
            if filename.startswith("1_"):
                target_name = "NVDA_1_sentinel_dashboard.html"
            elif filename.startswith("2_"):
                target_name = "NVDA_2_liquidity_heatmap.html"
            elif filename.startswith("3_"):
                target_name = "NVDA_3_volatility_cone.html"
            elif filename.startswith("4_"):
                target_name = "NVDA_4_sentiment_impact.html"
            elif filename.startswith("5_"):
                target_name = "NVDA_5_crash_gauge.html"
            else:
                continue
            
            if target_name not in found_files:
                found_files[target_name] = file
                print(f"✅ Found: {filename}")
    
    return found_files

def copy_plots(ticker="NVDA"):
    """Copy plots from Downloads to plots directory"""
    print("=" * 60)
    print(f"Copying preloaded plots for {ticker}")
    print("=" * 60)
    print()
    
    print(f"Source directory: {DOWNLOADS_DIR}")
    print(f"Destination directory: {PLOTS_DIR}")
    print()
    
    found_files = find_plot_files()
    
    if not found_files:
        print("❌ No plot files found in Downloads directory!")
        print()
        print("Expected files:")
        for generic_name in PLOT_MAPPING.keys():
            print(f"  - {generic_name}")
        print()
        print("Or files starting with 1_, 2_, 3_, 4_, 5_")
        return False
    
    print(f"\nFound {len(found_files)} plot file(s)")
    print()
    
    # Copy files
    copied = 0
    for target_name, source_path in found_files.items():
        dest_path = PLOTS_DIR / target_name
        
        try:
            shutil.copy2(source_path, dest_path)
            print(f"✅ Copied: {source_path.name} → {target_name}")
            copied += 1
        except Exception as e:
            print(f"❌ Error copying {source_path.name}: {e}")
    
    print()
    print("=" * 60)
    if copied > 0:
        print(f"✅ Successfully copied {copied} plot file(s)!")
        print()
        print("The dashboard should now display these plots.")
        print(f"Check: {PLOTS_DIR}")
    else:
        print("❌ No files were copied.")
    print("=" * 60)
    
    return copied > 0

if __name__ == "__main__":
    # Default to NVDA, but you can change this
    TICKER = "NVDA"
    copy_plots(TICKER)

