"""
流水线 3：多策略执行与信号整合。

负责运行所有启用的选股/风控策略，并整合筹码因子。
执行流程：
1. 批量运行所有策略 (Pass 1)：生成基础信号。
2. 筹码因子由 strategy_sig_chips 表直接提供，跳过回填与二次计算。
"""

import sys
import os
import logging
from datetime import datetime
from typing import List

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.strategies.runner import StrategyRunner

# 导入所有策略实现以触发注册
import ashare.strategies.trend_strategy       # ma5_ma20_trend
import ashare.strategies.wyckoff_strategy     # wyckoff_distribution
import ashare.strategies.low_suck_strategy    # low_suck_reversal


def run_strategy_pass(runner: StrategyRunner, force: bool, pass_name: str):
    """运行单个策略的单次扫描。"""
    try:
        runner.logger.info(f">>> [{pass_name}] 启动策略: {runner.strategy_code}")
        runner.run(force=force)
    except Exception as e:
        runner.logger.error(f"策略 {runner.strategy_code} 执行失败: {e}")


def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_3")
    
    # 定义需要运行的策略列表 (顺序很重要)
    # 核心策略放前面
    strategies_to_run = [
        "ma5_ma20_trend",
        "low_suck_reversal",
        "wyckoff_distribution"
    ]
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 3: 多策略扫描 - {datetime.now()}")
    logger.info(f"启用策略: {strategies_to_run}")
    logger.info("==============================================")

    try:
        force_env = str(os.getenv("ASHARE_STRATEGY_FORCE", "")).strip().lower()
        force = force_env in {"1", "true", "yes", "y", "on"}
        
        runners = {}
        # 初始化所有 Runner
        for code in strategies_to_run:
            try:
                runners[code] = StrategyRunner(strategy_code=code)
            except Exception as e:
                logger.error(f"初始化策略 {code} 失败: {e}")

        # --- 第 1 轮：所有策略生成基础信号 ---
        logger.info("\n=== 阶段 1: 生成基础信号 ===")
        for code in strategies_to_run:
            if code in runners:
                run_strategy_pass(runners[code], force, "Pass 1")

        logger.info("\n=== 阶段 2: 跳过筹码回填与二次决策 ===")
        
        logger.info("==============================================")
        logger.info("流水线 3: 所有策略执行完成！")
        logger.info("==============================================")

    except Exception as e:
        logger.exception("流水线 3 执行失败: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
