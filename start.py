"""AShare 全流程启动脚本。

负责按顺序调度以下流水线：
1. 数据抓取 (Pipeline 1)
2. 指标计算 (Pipeline 2)
3. 策略执行 (Pipeline 3) - 多策略并行 + 筹码因子

注意：OpenMonitor (开盘监测) 通常作为独立服务或定时任务运行，不在此脚本中默认启动。
"""

import argparse
import logging
import sys
import os

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ashare.utils.logger import setup_logger
from ashare.core.schema_manager import ensure_schema

# 导入各流水线入口
import scripts.pipeline_1_fetch_raw as p1
import scripts.pipeline_2_process_indicators as p2
import scripts.pipeline_3_run_strategy as p3


def main():
    parser = argparse.ArgumentParser(description="AShare 全流程启动脚本")
    parser.add_argument("--init-db", action="store_true", help="初始化数据库结构")
    parser.add_argument("--skip-p1", action="store_true", help="跳过 P1: 数据抓取")
    parser.add_argument("--skip-p2", action="store_true", help="跳过 P2: 指标计算")
    parser.add_argument("--skip-p3", action="store_true", help="跳过 P3: 策略扫描")
    
    args = parser.parse_args()

    setup_logger()
    logger = logging.getLogger("ashare.start")

    try:
        if args.init_db:
            logger.info(">>> [Init] 正在初始化/校验数据库结构...")
            ensure_schema()

        if not args.skip_p1:
            logger.info("\n>>> [P1] 启动流水线 1: 原始数据抓取...")
            # P1 内部会调用 ensure_schema，所以如果没传 --init-db 也会检查
            p1.main()

        if not args.skip_p2:
            logger.info("\n>>> [P2] 启动流水线 2: 指标计算与环境分析...")
            p2.main()

        if not args.skip_p3:
            logger.info("\n>>> [P3] 启动流水线 3: 多策略扫描与筹码分析...")
            p3.main()

        logger.info("\n==============================================")
        logger.info("🎉 AShare 全流程执行完毕！")
        logger.info("下一步建议：")
        logger.info("  - 运行开盘监测: python -m scripts.run_open_monitor")
        logger.info("==============================================")

    except SystemExit as e:
        if e.code != 0:
            logger.error("流程异常中断。\n")
            sys.exit(e.code)
    except Exception as e:
        logger.exception("全流程执行过程中发生未捕获异常: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()