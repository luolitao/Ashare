import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 sys.path，确保能导入模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.core.app import AshareApp
from scripts.run_daily_market_indicator import main as run_market_indicator
from scripts.run_index_weekly_channel import main as run_index_channel

def main():
    # 初始化日志配置
    setup_logger()
    # 获取当前模块的 logger
    logger = logging.getLogger("Pipeline_1_Data")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 1: 数据更新与环境分析 - {datetime.now()}")
    logger.info("==============================================")

    try:
        # 1. 采集原始数据 (Baostock/Akshare)
        logger.info(">>> 步骤 1/3: 运行 AshareApp 数据同步 (Fetch Data)...")
        app = AshareApp()
        app.run()
        logger.info(">>> 步骤 1/3 完成。")

        # 2. 更新全市场日线指标
        logger.info(">>> 步骤 2/3: 运行 run_daily_market_indicator (Market Indicators)...")
        run_market_indicator()
        logger.info(">>> 步骤 2/3 完成。")

        # 3. 计算指数周线通道（大盘环境）
        logger.info(">>> 步骤 3/3: 运行 run_index_weekly_channel (Market Environment)...")
        run_index_channel()
        logger.info(">>> 步骤 3/3 完成。")

        logger.info("==============================================")
        logger.info("流水线 1 执行成功！数据已更新完毕。")
        logger.info("请继续执行 pipeline_2_run_strategies.py")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 1 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
