#!/usr/bin/env python3
"""
Run RiskBeacon FastAPI Server
Simple script to start the API server for local development
"""
import sys
import os

# Add current directory to path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("RiskBeacon API Server")
    print("=" * 60)
    print("Starting server on http://localhost:8000")
    print("API Docs: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/health")
    print("=" * 60)
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()
    
    # Use import string for proper reload functionality
    uvicorn.run(
        "api.main:app",  # Import string instead of app object
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_excludes=["frontend/*", "*.pyc", "__pycache__/*", ".streamlit/*"]
    )

