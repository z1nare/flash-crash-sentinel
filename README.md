# RiskBeacon

**Real-Time Market Risk Monitoring System**

RiskBeacon is a Python-based real-time market risk monitoring system that ingests OHLCV (Open, High, Low, Close, Volume) market data, computes quantitative risk metrics, and provides an interactive dashboard for risk analysis. The system is designed for high-reliability risk monitoring where errors could misrepresent market stress conditions.

## 🎯 Overview

RiskBeacon provides:
- **Real-time risk metrics**: VPIN (Volume-synchronized Probability of Informed Trading), Yang-Zhang volatility, regime classification
- **Sentiment analysis**: FinBERT-based news sentiment analysis for market context
- **Interactive dashboard**: Streamlit-based visualization with Plotly charts
- **RESTful API**: FastAPI backend for programmatic access
- **Data persistence**: CSV-based storage for historical analysis
- **MLOps integration**: MLflow for model tracking and reproducibility 

## 🏗️ Architecture

```
RiskBeacon/
├── api/                    # FastAPI REST API
│   ├── main.py            # API entry point
│   ├── routes.py          # API endpoints
│   └── schemas.py         # Pydantic models
├── controllers/           # Business logic orchestration
│   └── ServiceController.py
├── services/              # Core services
│   ├── vpin_service.py   # VPIN calculation
│   ├── vol_service.py    # Volatility calculation
│   ├── regime_service.py  # Market regime detection
│   ├── sentimentService.py # News sentiment analysis
│   └── plotService.py    # Plot generation
├── frontend/              # Streamlit dashboard
│   └── dashboard.py
├── test/                  # Comprehensive test suite
│   ├── unit_testing/     # Unit tests
│   ├── integration_testing/ # Integration tests
│   ├── data_validation/  # Great Expectations suites
│   ├── mlops/            # MLflow tests
│   └── backtesting/      # Strategy backtesting
└── docs/                  # Documentation
    ├── requirements.md   # System requirements
    ├── PLAN.md          # Test plan
    └── portfolio/       # Coursework portfolio
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Conda (recommended) or virtualenv

### Installation

```powershell
# Create and activate environment
conda create -n riskbeacon python=3.11 -y
conda activate riskbeacon

# Navigate to project
cd python_components

# Install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

### Running the API

```powershell
cd python_components
python run_api.py
```

API will be available at:
- Health check: `http://127.0.0.1:8000/health`
- API docs: `http://127.0.0.1:8000/docs`

### Running the Dashboard

```powershell
cd python_components
python run_dashboard.py
```

Dashboard will open automatically in your browser at `http://localhost:8501`

## 📊 Key Features

### Risk Metrics

- **VPIN (Volume-synchronized Probability of Informed Trading)**: Measures order flow toxicity, clamped to [0,1]
- **Yang-Zhang Volatility**: High-frequency volatility estimator using OHLC data
- **Regime Classification**: ML-based market regime detection (bull/bear/neutral)

### Data Sources

- **Interactive Brokers**: Real-time market data feed (optional)
- **Historical Data**: CSV-based historical OHLCV data
- **News Sentiment**: FinBERT-based sentiment analysis from news headlines

### API Endpoints

Key endpoints:
- `POST /api/tick` - Ingest OHLCV tick data
- `POST /api/news` - Submit news for sentiment analysis
- `GET /api/metrics/history` - Retrieve historical metrics
- `POST /api/plots/generate` - Generate Plotly visualizations
- `GET /api/status` - System status and health

See `http://127.0.0.1:8000/docs` for complete API documentation.

## 🧪 Testing

The project includes a comprehensive test suite with **200+ tests** achieving **76% code coverage**.

### Running Tests

```powershell
cd python_components

# Run all tests with coverage
python -m pytest --cov=api --cov=controllers --cov=services --cov-report=term-missing --cov-report=html

# Run specific test categories
python -m pytest test/unit_testing/          # Unit tests
python -m pytest test/integration_testing/   # Integration tests
python -m pytest test/data_validation/       # Data validation (GX)
python -m pytest test/mlops/                # MLOps tests
```

### Test Coverage

- **Unit Tests**: Service-level correctness, edge cases, error handling
- **Integration Tests**: API endpoints, CSV persistence, plot generation
- **Property-Based Tests**: Quantitative invariants (Hypothesis)
- **Performance Tests**: Latency measurement (`/api/tick` median: 14.16ms)
- **Data Validation**: Great Expectations suites for data quality
- **Backtesting**: Strategy validation with train/test splits

See `docs/how_to_run.md` for detailed testing instructions.

## 📚 Documentation

- **Requirements**: `docs/requirements.md` - System requirements (RB-REQ-01..RB-REQ-17)
- **Test Plan**: `docs/PLAN.md` - Comprehensive test plan with inventory and risk register
- **Traceability**: `docs/requirements_to_tests.md` - Requirements → tests mapping
- **How to Run**: `docs/how_to_run.md` - Detailed setup and execution guide
- **Portfolio**: `docs/portfolio/ST_portfolio.tex` - Coursework portfolio (LaTeX)

## 🔧 Configuration

### Environment Variables

- `RISKBEACON_DISABLE_FINBERT=true` - Disable FinBERT downloads (useful for CI)
- `RISKBEACON_IB_HOST` - Interactive Brokers host (optional)
- `RISKBEACON_IB_PORT` - Interactive Brokers port (optional)

### Data Directories

- `historicalData/` - Historical OHLCV CSV files (per ticker)
- `dataInCsv/` - News sentiment data
- `plots/` - Generated Plotly HTML visualizations
- `experiments/regime_detection/models/` - Trained regime detection models

## 🏭 CI/CD

GitHub Actions workflows:
- **`.github/workflows/ci.yml`**: Continuous integration
  - Runs test suite with coverage
  - Exports Great Expectations suites
  - Uploads coverage artifacts
- **`.github/workflows/mutation.yml`**: Mutation testing (manual trigger)
  - Tests high-risk quant services
  - Generates mutation scores

