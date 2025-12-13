"""运行 MA5-MA20 顺势趋势波段系统。

用法：
  python run_ma5_ma20_trend_strategy.py

说明：
  - 读取 config.yaml 的 strategy_ma5_ma20_trend 配置。
  - 需要你已先运行 python start.py，把 history_recent_{N}_days 等表准备好。
"""

from ashare.ma5_ma20_trend_strategy import MA5MA20StrategyRunner


def main() -> None:
    MA5MA20StrategyRunner().run()


if __name__ == "__main__":
    main()
