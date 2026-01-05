# AI Skills Catalog (v2.0)

This directory contains executable python scripts designed to empower AI Agents with data access and system analysis capabilities.

## 🛠️ Infrastructure Skills

### [db_query.py](skills/db_query.py)
**Advanced SQL Executor.**
- **Meta-data Aware**: Returns execution time and total row counts.
- **Auto-limiting**: Selects are limited to 50 rows by default.
- **Shorthand**: `desc <tablename>` is supported.
- **Batching**: Supports multiple statements separated by `;`.

### [raw_reader.py](skills/raw_reader.py)
**Intelligent File Reader.**
- **Sampling**: Automatically provides Head/Tail samples for large files.
- **Structured**: Parses CSV/JSON into structured JSON summaries.
- **Recursive List**: `list` command provides a map of available data files.

### [env_tester.py](skills/env_tester.py)
**Data Quality Auditor.**
- **Anomaly Detection**: Flags volume unit mismatches (Hand vs Share).
- **Consistency**: Sync check between Daily K-lines and Indicators.
- **Depth Check**: Verifies if historical data is sufficient for MA250.

## 📈 Analysis Skills

### [market_env_analyzer.py](skills/market_env_analyzer.py)
**Market Context Reporter.**
- **Visual**: ASCII bar showing weekly risk levels.
- **Strategic**: Provides Plan A/B based on current market regime.

### [project_exporter.py](skills/project_exporter.py)
**Project Map Generator.**
- **Structure**: Generates a recursive file tree index.
- **Context Optimization**: Full-text for core logic, head-only for assets.