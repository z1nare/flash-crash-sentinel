#!/usr/bin/env python3
"""
Run RiskBeacon Streamlit Dashboard
Simple script to start the dashboard for local development
"""
import sys
import os
import subprocess

# Get script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

if __name__ == "__main__":
    print("=" * 60)
    print("RiskBeacon Dashboard")
    print("=" * 60)
    print("Starting Streamlit on http://localhost:8501")
    print("=" * 60)
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()
    
    # Run streamlit
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        "frontend/dashboard.py",
        "--server.port=8501",
        "--server.address=0.0.0.0",
        "--browser.gatherUsageStats=false"
    ])

