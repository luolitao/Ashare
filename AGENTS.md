# Repository Guidelines

## Project Structure and Module Organization
- `ashare/` contains core Python modules (data fetchers, strategies, monitoring, DB schema). Entry points and strategy runners import from here.
- `run_*.py` scripts run individual workflows such as `run_ma5_ma20_trend_strategy.py` or `run_open_monitor.py`.
- `start.py` runs the end-to-end pipeline.
- `config.yaml` holds environment, database, and proxy settings.
- `tool/` contains utility scripts for exporting and network checks (ad-hoc tests).

## Build, Test, and Development Commands
- Install dependencies: `pip install -r requirements.txt`
- Run full pipeline: `python start.py`
- Run a single module: `python run_ma5_ma20_trend_strategy.py` (swap in other `run_*.py` files)
- Open monitor scheduler: `python run_open_monitor_scheduler.py --interval 5`
- Network checks: `python tool/test_baostock_network.py`, `python tool/test_akshare_network.py`

## Coding Style and Naming Conventions
- Python style: 4-space indentation, PEP 8 naming, `snake_case` for functions/variables, `CamelCase` for classes.
- Module names are lowercase with underscores, matching the `ashare/` package.
- Keep configs in `config.yaml` and avoid hardcoding credentials.

## Testing Guidelines
- Run unit tests with `pytest` (install `requirements-dev.txt` first). Use `pytest -m requires_db` to include DB-backed tests.
- DB tests require real MySQL credentials via `MYSQL_HOST`, `MYSQL_USER`, and optional `MYSQL_PASSWORD` / `MYSQL_DB_NAME`.
- Network checks still use `tool/test_*.py` scripts for Baostock/AkShare connectivity.

## Commit and Pull Request Guidelines
- Commit messages are short and descriptive; type prefixes like `feat:` appear in history. Use a concise summary of the change.
- PRs should include a clear description, linked issues (if any), and expected behavior changes. Add screenshots/log snippets for monitoring or strategy output changes.

## Configuration and Data Notes
- `config.yaml` controls database connectivity and optional proxy settings. Keep local secrets out of version control.
- The system writes logs to `ashare.log`; include relevant excerpts when reporting issues.
