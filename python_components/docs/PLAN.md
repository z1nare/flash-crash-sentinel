# RiskBeacon Test Plan

**Author:** Arsenii Harbar  
**Project:** RiskBeacon (Real-Time Market Risk Monitoring System)  
**Date:** January 2026  
**Repo:** tests in `python_components/test/…`, requirements in `python_components/docs/requirements.md`, CI in `.github/workflows/ci.yml`.

## Introduction

So this test plan basically takes the LO1 requirements and turns them into actual tests and evidence for RiskBeacon. I'm doing requirements-driven testing (verification), but also checking that the dashboard actually works (validation) and tracking metrics to see how things are going (monitoring).

**Strategy vs plan:**

The strategy is the long-term stuff that doesn't change much - CI automation, conventions, the GitHub Actions workflow (`.github/workflows/ci.yml`), test folder structure, pinned dev dependencies. 

The plan is this document plus the test inventory and the evolution section that tracks real bugs I found and commits. This changes a lot as I discover issues.

**Context:** Main user is someone monitoring market stability - could be a student, trader, or risk analyst. The parts that really matter are: getting VPIN/vol calculations right, handling bad CSV data gracefully, dealing with IB connection failures, and keeping performance decent under load.

## Testing Approach

### Unit Testing

I'm testing individual math functions like VPIN and Yang-Zhang Volatility, plus data parsing logic.

Using `pytest` for this. For the math stuff, I'm using Hypothesis to check invariants plus regular unit tests on the same services - gives me redundancy. Also made sure to test edge cases like when trade volume is zero (gotta avoid division by zero in VPIN).

### Integration Testing

Testing how FastAPI, ServiceController, and the InteractiveBrokers adapter work together.

Using `pytest-mock` and `unittest.mock`. The code uses `RegimeModelInterface` and data provider interfaces which makes testing easier. I mock the Interactive Brokers API to test different scenarios:
- Normal market data stream (happy path)
- Network disconnection or timeout (need resilience here)
- Bad packets or invalid ticker symbols

Main goal is making sure the system doesn't crash the dashboard when IB fails.

### System Testing

End-to-end testing of the full pipeline: `POST /api/tick` → CSV persistence → metric computation → plots/dashboard.

Using FastAPI `TestClient` for this. Was thinking about Selenium/Playwright for Streamlit but didn't get to it.

## Test Inventory

Here's what tests I actually have and how they map to the LO1 requirements (`docs/requirements.md`):

| Test type | Purpose | Tooling | Evidence | Requirements |
|---|---|---|---|---|
| Unit | Correctness + regression | `pytest` | `test/unit_testing/service_modules/*` | RB-REQ-06/07/09 |
| Property-based | Quant invariants | `hypothesis` | `test/unit_testing/Quant_services/test_vpin_invariants.py`, `test/unit_testing/Quant_services/test_vol_yang_zhang_invariants.py` | RB-REQ-06/07 |
| Integration | Endpoint + filesystem | `fastapi.testclient` | `test/integration_testing/test_api_tick_persists_csv.py`, `test/integration_testing/test_api_plot_generation.py` | RB-REQ-01/04/14 |
| Performance | Latency/time | `pytest` + timers | `test/integration_testing/test_perf_api_tick_latency.py`, `test/integration_testing/test_perf_plot_generation_time.py` | RB-REQ-16/17 |
| Data validation | Schema + invariants | Great Expectations | `test/data_validation/gx_suites.py`, `test/data_validation/test_gx_market_data.py`, `test/data_validation/test_gx_sentiment_data.py` | RB-REQ-13/14 |
| MLOps | Reproducibility | MLflow | `test/mlops/test_mlflow_tracking.py` | RB-REQ-12 |
| Backtesting | Time-series validation | `bt` (Backtrader) | `test/backtesting/research.ipynb` | RB-REQ-11 |
| CI automation | Repeatability | GitHub Actions | `.github/workflows/ci.yml` | LO5 evidence |

**CI Artifacts:**

CI workflow uploads coverage reports (`coverage.xml`, `htmlcov/`) and GX suite exports (`docs/data_validation/gx_suites/`).

**What I've Done:**

Backtesting with time-series splits (RB-REQ-11) is in `test/backtesting/research.ipynb`. It does strategy backtesting with 70/30 train/test splits, includes sentiment integration, stop-loss, and supports multiple assets (AMD, NVDA, TSLA). Shows portfolio returns, Sharpe ratios, drawdown analysis, and prevents look-ahead bias.

**What I Haven't Done:**

Metamorphic regime tests (RB-REQ-10) - planned it but ran out of time. Maybe I'll add it later if I have time.

## Code Changes for Testability

Made several changes to make things more testable and robust. Added logging, better error handling, diagnostics, and ways to control behavior during testing.

**Plot pipeline fix:**

Plot generation was failing because of duplicate column names after normalization. Had `VPIN` + `vpin` which ended up as duplicate `vpin`. This caused non-1D column access errors. Fixed it in `services/plotService.py` by deduplicating normalized columns in `load_data`.

**CSV normalization tool:**

Created `normalize_historical_data.py` to fix corrupted CSVs. It repairs duplicated metric columns and recomputes derived metrics. Handy for cleaning up old data.

**Interactive Brokers stuff:**

Added logging and connection status checks so IB failures don't break the dashboard. Changes in:
- `services/ib_client_service.py` (logs, connection info)
- `controllers/ServiceController.py` (IB status + connect overrides)
- `api/routes.py` (IB endpoints)
- `frontend/dashboard.py` (IB connection settings UI)
- `ib_troubleshoot.py` (local troubleshooting tool - this was really useful)

**VPIN/regime improvements:**

Fixed VPIN bar-based classification to avoid saturation and keep outputs useful for regime detection. Added a rule-based regime mode (`REGIME_MODE=rule`) as a fallback when the ML model assumptions break. Changes in:
- `services/vpin_service.py`
- `controllers/ServiceController.py`

## Risk Register

Things that could go wrong and how I'm dealing with them:

| Risk | Likelihood | Impact | Mitigation | Test/Evidence |
|---|---|---|---|---|
| IB not running / wrong port | High | Medium | Feature flag + graceful degradation; troubleshooting script | `ib_troubleshoot.py`, `/api/ib/*` endpoints |
| IB market data permissions | Medium | Medium | Document the limitation; fall back to historical data | `ib_troubleshoot.py` logs; dashboard messaging |
| Malformed/duplicated CSV columns | High | High | Normalize columns on load; normalization script | `services/plotService.py`, `normalize_historical_data.py` |
| Model always predicts crash | Medium | High | VPIN heuristic fix; rule-based fallback | `services/vpin_service.py`, `REGIME_MODE=rule` |
| Heavy deps break CI (FinBERT/torch) | High | High | Disable via env var in CI | `.github/workflows/ci.yml`, `services/sentimentService.py` |
| Performance regressions | Medium | High | Performance tests; removed redundant CSV IO | `test/integration_testing/test_perf_*`, `services/vol_service.py` |
| Great Expectations version issues | Medium | Medium | Pin dev deps; export suites in CI | `requirements-dev.txt`, `scripts/export_gx_suites.py` |
| Large datasets not in repo | High | Medium | CI uses synthetic data; tests use temp data | `scripts/export_gx_suites.py`, TestClient tests |

## Monitoring

I'm tracking:
- CI pass rate (GitHub Actions)
- Coverage % and trend (`coverage.xml`, `htmlcov/`)
- Open defects (GitHub Issues or `docs/KNOWN_ISSUES.md` if I create one)
- Performance: `/api/tick` median/p95; plot generation time (printed by perf tests)

## Advanced Testing Techniques

### Property-Based Testing (Hypothesis)

Financial metrics need to follow math rules no matter what input data we get. Regular example-based tests can't cover all possible market price combinations, so I'm using Hypothesis.

**Tool:** `hypothesis`

**What I implemented:**
- **Invariant 1 (Volatility):** For any OHLC price sequence, Yang-Zhang volatility σ²_YZ must be ≥ 0.
- **Invariant 2 (VPIN):** VPIN must always be in [0, 1].

Evidence: `test/unit_testing/Quant_services/*`

### Mutation Testing

Need to measure test quality (yield). Gotta make sure tests actually catch bugs when the code is broken.

**Tool:** `mutmut` (tried `cosmic-ray` but went with mutmut - seemed easier to set up)

**Plan:**
- Run mutation analysis on `volatility_service.py` and `vpin_service.py`.
- **Target:** Get mutation score > 80% (kill 80% of mutants).
- **Example:** If code changes `VPIN < 0.5` to `VPIN <= 0.5`, is there a test at exactly 0.5 that fails?

### Metamorphic Testing (ML)

The "Oracle Problem" - we don't always know the "correct" regime for new data. So instead of testing exact values, I'm testing relationships between inputs and outputs.

**Metamorphic Relations:**
- **Relation 1 (Monotonicity):** If we take "Normal" data and add high volatility + toxic order flow, the "Crash" probability should increase or stay the same. Never decrease.
- **Relation 2 (Stability):** Adding small noise (< 0.01%) shouldn't flip classification from "Normal" to "Crash".

## CI/CD Pipeline

Using GitHub Actions. Workflow is in `.github/workflows/ci.yml` (repo root).

It installs dependencies, runs pytest with coverage, exports GX suite docs, uploads artifacts. Makes testing repeatable and visible. Prevents "big bang" integration issues and keeps results traceable.

## Traceability

RiskBeacon uses requirements `RB-REQ-01..RB-REQ-17` from `docs/requirements.md`, mapped to tests in `docs/requirements_to_tests.md`.

**Some examples:**

| Requirement ID | Description | Test component(s) | File location(s) |
|---|---|---|---|
| **RB-REQ-04** | Generate 5 plots per ticker | System | `test/integration_testing/test_api_plot_generation.py`, `services/plotService.py` |
| **RB-REQ-06** | VPIN in [0,1] | Unit/Property + GX | `test/unit_testing/Quant_services/test_vpin_invariants.py`, `test/data_validation/test_gx_market_data.py` |
| **RB-REQ-12** | Reproducible ML artifacts | MLOps (MLflow) | `test/mlops/test_mlflow_tracking.py` |
| **RB-REQ-15** | IB failures degrade gracefully | Integration | `ib_troubleshoot.py`, `api/routes.py`, `services/ib_client_service.py` |

## Plan Evolution

This plan changed as I found bugs during development and testing:

**Iteration 1 — Plot failure:**

Plot generation was failing because of duplicate column names after normalization (`VPIN` + `vpin` → duplicate `vpin`). Fixed plot loader, added GX schema checks, created `normalize_historical_data.py` to repair data. Evidence: `services/plotService.py`, `test/data_validation/*`, `normalize_historical_data.py` (commits: `bda8aec`, `0d5997f`).

**Iteration 2 — IB integration:**

IB connectivity depends on environment (ports, permissions) and was breaking the dashboard. Added IB endpoints, connection settings UI, `ib_troubleshoot.py`, made sure API works when IB fails. Evidence: `services/ib_client_service.py`, `api/routes.py`, `frontend/dashboard.py`, `ib_troubleshoot.py` (commit: `7fa6ada`).

**Iteration 3 — Regime/VPIN stability:**

VPIN classification was saturating; ML regimes were breaking with weird input distributions. Improved VPIN heuristic, added `REGIME_MODE=rule` fallback, strengthened data validation and MLflow tracking. Evidence: `services/vpin_service.py`, `controllers/ServiceController.py`, `test/mlops/test_mlflow_tracking.py` (commits: `2cceb5c`, `c1cae2d`).

## Limitations

**Data drift:** Tests use historical data. This doesn't guarantee future performance (covariate shift).

**Oracle limit:** We assume training labels are correct, but market regimes are somewhat subjective.

**Time-series constraint:** Using time-series splits for backtesting to prevent look-ahead bias (documented in backtesting code).
