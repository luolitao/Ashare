# Scripts

Run scripts via `python -m scripts.run_xxx` (recommended) or `python scripts/run_xxx.py`.

- `run_chip_filter.py`: run chip filter calculation.
- `run_daily_market_indicator.py`: backfill daily market indicators.
- `run_index_weekly_channel.py`: backfill weekly market indicators.
- `run_ma5_ma20_trend_strategy.py`: run MA5-MA20 trend strategy.
- `run_open_monitor.py`: run open monitor once.
- `run_open_monitor_scheduler.py`: schedule open monitor on intervals (skips non-trading days).
- `run_premarket_funnel.py`: premarket funnel (signal -> chip -> daily -> weekly).
