# AShare Project Context for Gemini

This document provides a comprehensive overview of the AShare quantitative analysis system to guide future AI interactions.

## 1. Project Overview

**AShare** is a Python-based quantitative trading and analysis system for the Chinese A-Share market. It integrates data collection, strategy execution, risk control, and real-time market monitoring.

### Core Technologies
- **Language**: Python 3.13+
- **Data Sources**: Baostock (primary), AkShare (supplementary/real-time)
- **Database**: MySQL (via SQLAlchemy & PyMySQL)
- **Data Processing**: Pandas, NumPy
- **Architecture**: Modular design separating Data, Strategies, Indicators, and Monitoring.

## 2. Directory Structure

- **`ashare/`**: Main package source code.
    - **`core/`**: Application infrastructure (DB connection, Config parsing, Schema management).
    - **`data/`**: Data fetchers and storage logic.
    - **`strategies/`**: Trading strategies (e.g., `ma5_ma20_trend_strategy`, `chip_filter`).
    - **`monitor/`**: Real-time market monitoring and environment evaluation (`open_monitor`).
    - **`indicators/`**: Market breadth, sentiment, and technical indicator calculations.
    - **`utils/`**: Shared utilities (logging, conversion).
- **`scripts/`**: Executable scripts for individual modules (e.g., `run_open_monitor.py`).
- **`tool/`**: Utility scripts for testing, exporting, or analyzing local data.
- **`config.yaml`**: Main configuration file (DB credentials, strategy parameters).
- **`start.py`**: The primary entry point to run the full workflow.

## 3. Development & Operational Conventions

### Running Scripts
The project uses absolute imports (e.g., `from ashare.core...`). To avoid `ModuleNotFoundError`:
1.  **Preferred Method**: Run scripts as modules from the project root.
    ```bash
    python -m scripts.run_open_monitor
    python -m scripts.run_ma5_ma20_trend_strategy
    ```
2.  **Direct Execution**: Key scripts in `scripts/` have been patched to append the project root to `sys.path`.
    ```bash
    python scripts/run_open_monitor.py
    ```

### Database & Data
- **MySQL**: The system relies heavily on a MySQL database. Tables are managed via `ashare.core.schema_manager`.
- **Local Data**: Large datasets and output files (JSON/CSV in `tool/output/`) are listed in `.gitignore`.
    - **AI Interaction Note**: If you need to analyze these ignored files, **do not** rely on standard file reading tools. Instead, write and execute a temporary Python script to read and print the data.

### Configuration
- All configurable parameters (DB settings, Strategy thresholds) are in `config.yaml`.
- Strategies often have a `Params` dataclass (e.g., `MA5MA20Params`) that loads defaults from this file.

## 4. Key Workflows

### A. Full Daily Update
Run `python start.py` to:
1.  Fetch latest daily K-line data.
2.  Run strategies (MA5/MA20) to generate signals.
3.  Calculate market environment indicators.
4.  Update ready-to-trade signal lists.

### B. Open Monitor (Real-time)
Run `python -m scripts.run_open_monitor` to:
1.  Load "BUY" signals from the previous trading day.
2.  Fetch real-time quotes (Snapshot).
3.  Evaluate market environment (Gate).
4.  Output Action (EXECUTE/WAIT/STOP).
    - **Note**: CSV export for this module has been disabled by default. Results are persisted to `strategy_open_monitor_eval`.

### C. Strategy Development
- **New Strategies**: Place in `ashare/strategies/`.
- **Circular Imports**: Be cautious of circular dependencies between Strategy classes and Candidate services. Use separate `_params.py` files for configuration dataclasses if necessary.

## 5. Troubleshooting Common Issues

- **`ModuleNotFoundError: No module named 'ashare'`**:
    - Cause: Running a script from a subdirectory without correct `PYTHONPATH`.
    - Fix: Run from root using `python -m ...` or ensure the script adds `sys.path`.
- **`IntegrityError` (Duplicate Entry)**:
    - Cause: Re-running a strategy/monitor for the same date/run_id.
    - Fix: This is often expected behavior. The system typically handles idempotency, but manual re-runs might trigger DB constraints.
