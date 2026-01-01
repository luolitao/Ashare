# AShare Project Guidelines for Agents

> **Language Preference**: Please interact with the user in **Chinese (Simplified)**.

## 1. Tooling (AI Skills)
Specialized tools are located in `.ai/skills/`.
- Access ignored data: `python .ai/skills/raw_reader.py read <path>`

## 2. Project Layout
- `ashare/`: Main package.
- `scripts/`: Execution scripts.
- `.ai/`: Agent standards and skill implementations.

## 3. Development Commands
- Run Strategy: `python -m scripts.run_ma5_ma20_trend_strategy`
- Run Monitor: `python -m scripts.run_open_monitor`

## 4. Coding Standards
- PEP 8 compliance.
- Absolute imports only.
