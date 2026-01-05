import sys
import os
import logging
import datetime as dt
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.core.app import AshareApp
from ashare.indicators.market_indicator_runner import MarketIndicatorRunner
from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.indicators.market_indicator_builder import MarketIndicatorBuilder

def run_indicators_task(logger: logging.Logger):
    """
    执行核心指标计算任务（直接调用，方便调试）
    """
    try:
        mon = MA5MA20OpenMonitorRunner()
        builder = MarketIndicatorBuilder(env_builder=mon.env_builder, logger=logger)
        mi = MarketIndicatorRunner(repo=mon.repo, builder=builder, logger=logger)

        # 推断日期
        app = AshareApp()
        latest_date = app._infer_latest_trade_day_from_db('history_daily_kline')

        logger.info(f">>> 正在计算技术指标 (latest_date={latest_date})...")
        mi.run_technical_indicators(latest_date=latest_date)

        logger.info(f">>> 正在计算周线大盘趋势...")
        mi.run_weekly_indicator(mode='incremental')

        logger.info(f">>> 正在计算日线市场环境...")
        daily_mode = os.getenv("ASHARE_DAILY_INDICATOR_MODE", "incremental").strip().lower() or "incremental"
        daily_start = os.getenv("ASHARE_DAILY_INDICATOR_START", "").strip() or None
        daily_end = os.getenv("ASHARE_DAILY_INDICATOR_END", "").strip() or None
        logger.info(
            "日线指标参数：mode=%s start=%s end=%s",
            daily_mode,
            daily_start or "-",
            daily_end or "-",
        )
        mi.run_daily_indicator(
            mode=daily_mode,
            start_date=daily_start,
            end_date=daily_end,
        )
        
        return True
    except Exception as e:
        logger.exception(f"指标计算过程中发生错误: {e}")
        return False

def main():
    setup_logger()
    # 使用 'ashare' logger 以确保日志级别正确（Root logger 默认为 WARNING，会吞掉 INFO）
    logger = logging.getLogger("ashare")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 2: 数据预处理与指标计算 - {datetime.now()}")
    logger.info("==============================================")

    # 1. 刷新交易 Universe
    try:
        logger.info(f">>> 正在启动步骤: Building Universe")
        AshareApp().run_universe_builder()
        logger.info(f">>> 步骤 Building Universe 已成功完成。")
    except Exception as e:
        logger.error(f">>> 步骤 Building Universe 失败: {e}")
        # Universe 构建失败通常不应阻断后续（如果是增量更新），但这里为了安全起见可以选择继续或退出
        # 暂时选择继续

    # 2. 核心指标批量加工
    logger.info(f">>> 正在启动步骤: Full Processing & Indicators")
    if run_indicators_task(logger):
        logger.info("==============================================")
        logger.info("流水线 2: 所有指标加工任务执行成功！")
        logger.info("==============================================")
    else:
        logger.error("流水线 2: 指标计算任务失败。")
        sys.exit(1)

if __name__ == "__main__":
    main()
