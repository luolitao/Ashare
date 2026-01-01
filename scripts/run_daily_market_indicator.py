"""回填日线市场指标。"""

from __future__ import annotations

from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder
from ashare.indicators.market_indicator_runner import MarketIndicatorRunner
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.core.schema_manager import ensure_schema


def run_daily_market_indicator(
    *, start_date: str | None = None, end_date: str | None = None, mode: str = "incremental"
) -> dict:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()
    builder = MarketIndicatorBuilder(env_builder=runner.env_builder, logger=runner.logger)
    indicator_runner = MarketIndicatorRunner(
        repo=runner.repo,
        builder=builder,
        logger=runner.logger,
    )
    return indicator_runner.run_daily_indicator(
        start_date=start_date,
        end_date=end_date,
        mode=mode,
    )


def main() -> None:
    run_daily_market_indicator()


if __name__ == "__main__":
    main()
