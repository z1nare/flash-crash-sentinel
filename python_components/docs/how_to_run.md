# How to run RiskBeacon (local + tests)

This repo is structured with a `python_components/` subproject. Run commands from **`python_components/`** unless stated otherwise.

## 1) Install

### 1.1 Create/activate an environment (example: conda)

```powershell
conda create -n riskbeacon python=3.11 -y
conda activate riskbeacon
```

### 1.2 Install runtime + dev/testing dependencies

```powershell
cd "C:\Uni\Year 3\flash-crash-sentinel\python_components"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

Notes:
- The test suite assumes Great Expectations is pinned via `requirements-dev.txt`.
- CI disables FinBERT downloads using `RISKBEACON_DISABLE_FINBERT=true`.

## 2) Run the backend API (FastAPI)

```powershell
cd "C:\Uni\Year 3\flash-crash-sentinel\python_components"
python run_api.py
```

Then visit:
- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/docs` (OpenAPI)

## 3) Run the dashboard (Streamlit)

```powershell
cd "C:\Uni\Year 3\flash-crash-sentinel\python_components"
python run_dashboard.py
```

## 4) Run tests

### 4.1 Unit tests (fast, deterministic)

```powershell
cd "C:\Uni\Year 3\flash-crash-sentinel\python_components"
$env:RISKBEACON_DISABLE_FINBERT="true"
python -m pytest -q test\unit_testing -s
```

### 4.2 Integration tests (FastAPI TestClient)

```powershell
$env:RISKBEACON_DISABLE_FINBERT="true"
python -m pytest -q test\integration_testing -s
```

### 4.3 Performance evidence (RB-REQ-16/RB-REQ-17)

Measure-only by default (CI-safe). Set `PERF_STRICT=true` to enforce targets.

```powershell
$env:RISKBEACON_DISABLE_FINBERT="true"
python -m pytest -s test\integration_testing\test_perf_api_tick_latency.py
python -m pytest -s test\integration_testing\test_perf_plot_generation_time.py
```

### 4.4 Data validation (Great Expectations)

```powershell
python -m pytest -q test\data_validation -s
```

Export suite artifacts (JSON + summary in `docs/data_validation/gx_suites/`):

```powershell
python -m test.data_validation.run_data_validation
```

### 4.5 MLOps evidence (MLflow)

```powershell
$env:RISKBEACON_DISABLE_FINBERT="true"
python -m pytest -q test\mlops\test_mlflow_tracking.py -s
```

### 4.6 FinBERT / sentiment (heavy, optional)

By default, tests disable FinBERT so your suite is fast and CI-safe. To actually run
FinBERT locally:

- Ensure runtime deps are installed (`requirements.txt` includes `transformers` + `torch`)
- Allow model download (first run can be large/slow)
- Run the smoke test explicitly:

```powershell
$env:RISKBEACON_DISABLE_FINBERT=""
$env:RUN_FINBERT_TESTS="true"
python -m pytest -q test\unit_testing\service_modules\test_finbert_smoke.py -s
```

Tip: to cache downloads, optionally set:

```powershell
$env:TRANSFORMERS_CACHE="$PWD\.hf_cache"
```

## 5) Coverage evidence (portfolio)

```powershell
$env:RISKBEACON_DISABLE_FINBERT="true"
python -m pytest --cov=api --cov=controllers --cov=services --cov-report=term-missing --cov-report=xml --cov-report=html
```

Outputs:
- `coverage.xml`
- `htmlcov/` (open `htmlcov/index.html`)

## 6) Optional: Interactive Brokers (IB) troubleshooting

IB is optional and depends on local gateway/TWS + permissions.

```powershell
python ib_troubleshoot.py --gateway-paper
python ib_troubleshoot.py --gateway-paper --subscribe NVDA --seconds 15
```


