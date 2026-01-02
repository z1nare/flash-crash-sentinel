# RiskBeacon Requirements (Coursework Evidence for LO1)

This document defines a **testable** set of requirements for RiskBeacon, intended for the Software Testing coursework portfolio (LO1).

## Scope and context
RiskBeacon is a data pipeline and dashboard for real-time market risk monitoring:
- **Ingestion**: `POST /api/tick` receives OHLCV bars (simulated, historical replay, or IB real-time bars).
- **Processing**: `ServiceController.process_tick()` persists data to `historicalData/{TICKER}.csv` and triggers VPIN/volatility/regime logic.
- **Outputs**: `GET /api/metrics/history`, plot HTML generation via `POST /api/plots/generate`, and Streamlit dashboard visualisation.

### Requirement ID format
Requirements are labelled `RB-REQ-XX`. Each requirement includes:
- **Level**: System / Integration / Unit
- **Category**: Functional / Quant correctness / ML-MLOps / Robustness / Performance
- **Acceptance criteria**: what must be true for a pass

---

## A) Functional requirements (system-level)

### RB-REQ-01 — Tick ingestion persists OHLCV
- **Level**: System
- **Category**: Functional
- **Statement**: `POST /api/tick` shall persist the OHLCV record to the ticker-specific CSV at `historicalData/{TICKER}.csv`.
- **Acceptance criteria**:
  - CSV exists after first tick for a ticker.
  - Last row contains matching `timestamp,ticker,open,high,low,close,volume` (within parsing tolerance).

### RB-REQ-02 — Tick ingestion triggers metric computation when bucket fills
- **Level**: Integration
- **Category**: Functional / Quant pipeline
- **Statement**: When VPIN bucket conditions are met, the system shall compute and persist `VPIN` and `vol` (and optionally `regime` fields) to the ticker CSV.
- **Acceptance criteria**:
  - After sufficient ticks, at least one row contains non-null `VPIN` and non-null `vol`.

### RB-REQ-03 — Metrics history endpoint returns consistent schema
- **Level**: System
- **Category**: Functional
- **Statement**: `GET /api/metrics/history?ticker=...` shall return a JSON array whose elements include `timestamp,ticker,vpin,volatility` (and optionally `regime,regime_label,regime_confidence`).
- **Acceptance criteria**:
  - Response is HTTP 200 for existing ticker file.
  - Payload elements validate against the API schema and contain parseable timestamps.

### RB-REQ-04 — Plot generation produces 5 plot files for chosen ticker
- **Level**: System
- **Category**: Functional
- **Statement**: `POST /api/plots/generate` shall generate 5 HTML plot files for the requested ticker.
- **Acceptance criteria**:
  - Output directory contains exactly the expected ticker-prefixed filenames:
    - `{TICKER}_1_sentinel_dashboard.html`
    - `{TICKER}_2_liquidity_heatmap.html`
    - `{TICKER}_3_volatility_cone.html`
    - `{TICKER}_4_sentiment_impact.html`
    - `{TICKER}_5_crash_gauge.html`

### RB-REQ-05 — Dashboard displays selected ticker and updates metrics
- **Level**: System
- **Category**: Functional / UX correctness
- **Statement**: The Streamlit dashboard shall display the selected ticker and show latest metrics consistent with the API/CSV.
- **Acceptance criteria**:
  - Selected ticker changes displayed charts/metrics.
  - Latest VPIN/Volatility displayed equals most recent API metrics (within tolerance).

---

## B) Quant correctness requirements (unit-level, measurable)

### RB-REQ-06 — VPIN boundedness invariant
- **Level**: Unit
- **Category**: Quant correctness (invariant)
- **Statement**: VPIN values produced by `VpinService` shall always be in the interval \([0,1]\).
- **Acceptance criteria**:
  - For randomized/generated OHLC+volume sequences, computed VPIN never < 0 or > 1.

### RB-REQ-07 — Yang–Zhang volatility non-negativity
- **Level**: Unit
- **Category**: Quant correctness (invariant)
- **Statement**: Yang–Zhang realized volatility shall never be negative.
- **Acceptance criteria**:
  - For generated OHLC sequences with positive prices, volatility ≥ 0 and finite.

### RB-REQ-08 — Volatility robustness to malformed rows
- **Level**: Unit
- **Category**: Robustness / Quant correctness
- **Statement**: Volatility computation shall not return NaN/Inf when the dataset contains malformed rows (bad timestamps, missing OHLC, bad lines).
- **Acceptance criteria**:
  - Output volatility series contains only finite values (or zeros where data is insufficient).

### RB-REQ-09 — Crash probability boundedness
- **Level**: Unit
- **Category**: Quant correctness (invariant)
- **Statement**: The crash probability index produced by the plot pipeline shall be bounded in \([0,1]\).
- **Acceptance criteria**:
  - `plotService.plot_crash_probability` computes `crash_prob` within [0,1] for any valid input dataframe.

---

## C) ML / MLOps requirements

### RB-REQ-10 — Metamorphic stability under small perturbations
- **Level**: Unit/Integration
- **Category**: ML validity (metamorphic)
- **Statement**: Small perturbations to prices (e.g., < 0.01% noise) shall not frequently flip regime classification from Normal ↔ Crash.
- **Acceptance criteria**:
  - Over a test window, flip rate stays below a defined threshold (e.g., < 5% of points).

### RB-REQ-11 — No look-ahead bias in evaluation split
- **Level**: System (ML evaluation)
- **Category**: ML validity
- **Statement**: Model evaluation/backtesting shall use a time-series split (chronological) rather than random K-fold when reporting performance.
- **Acceptance criteria**:
  - Evaluation script explicitly uses chronological split and documents it.

### RB-REQ-12 — Reproducible model artifacts
- **Level**: System (MLOps)
- **Category**: MLOps
- **Statement**: Model training/evaluation shall be reproducible: artifacts are versioned and tied to code + data versions (seed, git commit, dataset hash).
- **Acceptance criteria**:
  - Metadata contains seed + data version + code version; rerun produces identical (or tightly bounded) metrics.

---

## D) Reliability / robustness requirements

### RB-REQ-13 — CSV ingestion tolerates bad lines
- **Level**: Integration
- **Category**: Robustness
- **Statement**: The pipeline shall tolerate malformed CSV rows without crashing API endpoints.
- **Acceptance criteria**:
  - API endpoints do not throw 500 due solely to a small number of malformed lines.

### RB-REQ-14 — Missing sentiment does not break plot generation
- **Level**: System
- **Category**: Robustness
- **Statement**: Plot generation shall succeed if sentiment data is missing or has no matching rows for a ticker.
- **Acceptance criteria**:
  - `POST /api/plots/generate` returns success and creates plot files even when sentiment rows are absent.

### RB-REQ-15 — IB connectivity failures degrade gracefully
- **Level**: System
- **Category**: Robustness / External integration
- **Statement**: If IB connection fails (port closed, permission errors, etc.), the API and dashboard shall remain operational using historical/simulated data.
- **Acceptance criteria**:
  - `/health` and dashboard load succeed even if `/api/ib/connect` fails.
  - Failure is surfaced as a user-facing message; no crash.

---

## E) Performance requirements (targets)


### RB-REQ-16 — Tick processing latency target
- **Level**: System
- **Category**: Performance
- **Statement**: For local runs, `POST /api/tick` shall be **fast**: median latency < **50 ms** and p95 < **150 ms** for a representative ticker dataset.
- **Acceptance criteria**:
  - Over 100 requests, median latency < 50 ms and p95 < 150 ms.

### RB-REQ-17 — Plot generation time target
- **Level**: System
- **Category**: Performance
- **Statement**: For local runs, `POST /api/plots/generate` shall complete within **300 seconds** for dataset size **3000 rows**.
- **Acceptance criteria**:
  - Wall-clock time < 300 seconds; plots produced correctly.


