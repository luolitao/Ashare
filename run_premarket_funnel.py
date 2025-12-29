"""盘前漏斗总控脚本：依次跑基础信号/筹码/日线/周线。"""

from __future__ import annotations

from ashare.utils.logger import setup_logger
from run_chip_filter import main as run_chip_filter
from run_daily_market_indicator import main as run_daily_market_indicator
from run_index_weekly_channel import main as run_index_weekly_channel
from run_ma5_ma20_trend_strategy import main as run_ma5_ma20_trend_strategy


def _run_step(name: str, func):
    logger = setup_logger()
    logger.info("[premarket] start: %s", name)
    result = func()
    logger.info("[premarket] done: %s", name)
    return result


def main() -> None:
    logger = setup_logger()
    try:
        _run_step("ma5_ma20_trend", run_ma5_ma20_trend_strategy)
        chip_rows = _run_step("chip_filter", run_chip_filter)
        logger.info("[premarket] chip_filter rows=%s", chip_rows)
        _run_step("daily_market_indicator", run_daily_market_indicator)
        _run_step("index_weekly_channel", run_index_weekly_channel)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[premarket] step failed, aborting: %s", exc)
        raise


if __name__ == "__main__":
    main()
