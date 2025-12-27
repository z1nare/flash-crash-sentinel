"""
FastAPI Application Entry Point
Wraps existing services (VPIN, Volatility, Sentiment) via ServiceController
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from controllers.ServiceController import ServiceController
from api.routes import router
import os

# Initialize FastAPI app
app = FastAPI(
    title="RiskBeacon API",
    description="Risk monitoring API for VPIN, Volatility, and Sentiment analysis",
    version="1.0.0"
)

# CORS middleware (allow frontend to connect)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global ServiceController instance (singleton)
service_controller = ServiceController()

# Set controller in routes module
from api.routes import set_controller
set_controller(service_controller)

# Include routes
app.include_router(router)

# Mount static files for plots (if plots directory exists)
plots_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "plots")
if os.path.exists(plots_dir):
    app.mount("/plots", StaticFiles(directory=plots_dir), name="plots")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "RiskBeacon API",
        "version": "1.0.0"
    }

@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "services": {
            "vpin": "ready",
            "volatility": "ready",
            "sentiment": "ready"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

