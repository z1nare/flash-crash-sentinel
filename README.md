## Flash Crash Sentinel

First-draft research prototype for market risk monitoring (API + dashboard).

This public repository intentionally ships **code only**. Large datasets, logs, plots, and trained models are **not included**.

## Minimal usage

- Install dependencies:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r python_components/requirements.txt
```

- Run API:

```bash
python python_components/run_api.py
```

- Run dashboard:

```bash
python python_components/run_dashboard.py
```

## Data / models

Use `python_components/download_data.py` (requires your own Kaggle credentials). Do not commit any downloaded files or credentials.

## Security notes

- Do not commit secrets (`.env`, `kaggle.json`, API keys, tokens).
- This project is **not hardened** for untrusted networks. Run locally behind a firewall/VPN.
