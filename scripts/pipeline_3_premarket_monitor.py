import sys
import os
import logging
import pandas as pd
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from scripts.run_open_monitor import main as run_open_monitor

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_3_Monitor")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 3: 盘前计划与实时监控 - {datetime.now()}")
    logger.info("==============================================")

    try:
        # 1. 检查是否有新的交易计划
        # 读取昨晚生成的 tool/output/strategy_results_YYYYMMDD.csv
        # 这里为了简单，我们让 run_open_monitor 自己去读库或者读配置
        # 但通常这里应该有一个 "将选股结果注入到监控列表" 的过程
        
        logger.info(">>> 步骤 1/1: 启动盘中监控 (Open Monitor)...")
        logger.info("注意：请确保 strategy_signals 表中已更新昨日选出的目标股。")
        
        # 启动监控
        run_open_monitor()
        
        logger.info("==============================================")
        logger.info("流水线 3 监控结束。")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 3 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
