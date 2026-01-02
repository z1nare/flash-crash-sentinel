# Test Strategy: RiskBeacon

**Author:** [Your Name]
**Project:** RiskBeacon - Real-Time Market Risk Monitoring System
**Date:** January 2026

## 1. Introduction
This document outlines the testing strategy for RiskBeacon, an institutional-grade risk monitoring system. The strategy moves beyond standard functional verification to include advanced techniques suitable for financial software, including **Property-Based Testing** for quantitative metrics, **Mutation Testing** for test suite robustness, and **Metamorphic Testing** for the Machine Learning components.

## 2. Testing Pyramid & Scope

### 2.1 Unit Testing (Foundational)
* **Scope:** Individual mathematical functions (VPIN, Yang-Zhang Volatility) and data parsing logic.
* **Tools:** `pytest`
* **Distinction Approach:**
    * [cite_start]**Math Verification:** Verify Equation (2) (VPIN) [cite: 85] [cite_start]and Equation (3) (Yang-Zhang) [cite: 87] against manual calculations in Excel/Spreadsheets to ensure implementation correctness.
    * **Boundary Analysis:** Test behavior when trade volume is zero (division by zero protection in VPIN formula).

### 2.2 Integration Testing (Mocking Strategy)
* **Scope:** Interaction between `FastAPI`, `ServiceController`, and the new `InteractiveBrokers` adapter.
* **Tools:** `pytest-mock`, `unittest.mock`
* **Strategy:**
    * [cite_start]**Dependency Injection:** As defined in the architecture[cite: 51], we utilize `RegimeModelInterface` and data provider interfaces.
    * **Mocking External Feeds:** We mock the Interactive Brokers API to simulate:
        * **Scenario A:** Standard market data stream (Happy Path).
        * **Scenario B:** Network disconnection/timeout (Resilience).
        * **Scenario C:** Malformed packets/Invalid Ticker symbols.
    * **Goal:** Ensure the system degrades gracefully without crashing the Dashboard when the data feed is interrupted.

### 2.3 System Testing (End-to-End)
* [cite_start]**Scope:** The full data pipeline from `POST /api/tick` to the Streamlit Dashboard update[cite: 63].
* **Tools:** `TestClient` (FastAPI), potentially Selenium/Playwright for Streamlit.

---

## 3. Advanced Testing Techniques (Distinction Evidence)

### 3.1 Property-Based Testing (Hypothesis)
* **Rationale:** Financial metrics must adhere to mathematical invariants regardless of input data. Standard example-based tests are insufficient to cover the infinite state space of market prices.
* **Tool:** `hypothesis`
* **Implementation Plan:**
    * **Invariant 1 (Volatility):** For any sequence of High/Low/Open/Close prices generated, the Yang-Zhang volatility $\sigma^2_{YZ}$ must always be $\geq 0$.
    * [cite_start]**Invariant 2 (VPIN):** The VPIN metric must always fall within the range $[0, 1]$, regardless of buy/sell volume distribution[cite: 85].
    * **Invariant 3 (Crash Preservation):** If the input stream contains `NaN` or `Infinite` values, the system must handle them (raise specific error or filter) rather than returning a 500 Server Error.

### 3.2 Mutation Testing
* **Rationale:** To quantitatively measure the quality of the test suite (Yield). We must ensure that our tests actually fail if the underlying logic is broken.
* **Tool:** `mutmut` or `cosmic-ray`
* **Implementation Plan:**
    * Run mutation analysis on the `volatility_service.py` and `vpin_service.py` modules.
    * **Target:** Achieve a mutation score > 80% (i.e., kill 80% of generated mutants).
    * [cite_start]**Example Mutant:** If the code changes `VPIN < 0.5` (Regime 0 boundary) [cite: 90] to `VPIN <= 0.5`, is there a test case specifically at `0.5` that fails?

### 3.3 Metamorphic Testing (Machine Learning)
* **Rationale:** It is the "Oracle Problem"—we do not always know the "correct" regime for unseen data.
* **Technique:** We test relationships between inputs and outputs.
* **Metamorphic Relations:**
    * **Relation 1 (Monotonicity):** If we take a historical data slice classified as "Normal" and artificially inject high volatility and toxic order flow (perturbation), the probability of "Crash" regime classification should *increase* or stay the same. It should never *decrease*.
    * **Relation 2 (Stability):** Adding white noise (random small variations < 0.01%) to the price data should not flip the classification from "Normal" to "Crash".

---

## 4. CI/CD Pipeline Automation

* **Platform:** GitHub Actions / GitLab CI
* **Workflow:**
    1.  **Static Analysis:** `ruff` (linting) and `bandit` (security scan for API keys).
    2.  **Unit & Integration:** Run `pytest` with coverage.
    3.  **Threshold Enforcement:** Pipeline fails if Coverage < 85%.
    4.  **Property Checks:** Run `hypothesis` suite (slower tests).

## 5. Traceability Matrix

| Requirement ID | Description | Test Component | File Location |
| :--- | :--- | :--- | :--- |
| **REQ-01** | [cite_start]Calculate VPIN per Eq(2) [cite: 85] | Unit / Property | `tests/unit/test_vpin.py` |
| **REQ-02** | Detect Regime Shift < 5 mins | Performance | `tests/perf/test_latency.py` |
| **REQ-03** | Handle IB Connection Drop | Integration | `tests/int/test_broker_mock.py` |
| **REQ-04** | [cite_start]Yang-Zhang Volatility Calc [cite: 87] | Unit / Property | `tests/unit/test_volatility.py` |
| **REQ-05** | [cite_start]ML Model Swap (Interface) [cite: 22] | Architecture | `tests/arch/test_interfaces.py` |

---

## 6. Evaluation of Limitations (LO4)
* [cite_start]**Data Drift:** Tests use historical Bloomberg data[cite: 79]. We acknowledge that this does not guarantee future performance (Covariate Shift).
* **Oracle Limit:** We assume the "Ground Truth" labels in the training set are correct, but market regimes are subjective.
* [cite_start]**Time-Series Constraint:** Standard K-Fold cross-validation was used[cite: 93], but Time-Series Split is more appropriate for backtesting to prevent look-ahead bias. This is a noted limitation in the current test plan.