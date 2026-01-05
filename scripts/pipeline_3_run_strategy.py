"""
流水线 3：多策略执行与信号整合。

负责运行所有启用的选股/风控策略，并整合筹码因子。
执行流程：
1. 批量运行所有策略 (Pass 1)：生成基础信号。
2. 计算筹码因子 (ChipFilter)：为所有新生成的信号计算筹码评分。
3. 重新运行核心策略 (Pass 2)：读取筹码因子，生成最终决策 (目前仅 ma5_ma20_trend 需要此步骤)。
"""

import sys
import os
import logging
from datetime import datetime
from typing import List

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text

from ashare.utils.logger import setup_logger
from ashare.strategies.runner import StrategyRunner
from ashare.strategies.chip_filter import ChipFilter

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


def run_chip_filter_for_all(runner_template: StrategyRunner, strategies: List[str]):
    """为所有策略的最新信号计算筹码因子。"""
    logger = runner_template.logger
    repo = runner_template.db_writer
    params = runner_template.params
    
    signal_table = params.get("signal_events_table")
    indicator_table = params.get("indicator_table")
    
    if not signal_table or not indicator_table:
        logger.warning("筹码计算跳过：表名未配置。")
        return

    # 获取所有策略中最新的日期 (取最大值)
    stmt_max = text(f"SELECT MAX(`sig_date`) AS max_d FROM `{signal_table}`")
    with repo.engine.begin() as conn:
        row = conn.execute(stmt_max).mappings().first()
    latest_date = row.get("max_d") if row else None
    
    if not latest_date:
        logger.warning("筹码计算跳过：未找到任何信号日期。")
        return
        
    logger.info(f"正在为日期 {latest_date} 计算筹码因子...")

    # 读取所有策略当天的信号
    stmt = text(
        f"""
        SELECT
          e.`sig_date`,
          e.`code`,
          e.`strategy_code`,
          e.`signal`,
          e.`final_action`,
          e.`risk_tag`,
          ind.`close`,
          ind.`ma20`,
          ind.`vol_ratio`,
          ind.`atr14`,
          ind.`macd_hist`,
          ind.`kdj_k`,
          ind.`rsi14`,
          ind.`ma20_bias`,
          ind.`yearline_state`
        FROM `{signal_table}` e
        LEFT JOIN `{indicator_table}` ind
          ON ind.`trade_date` = e.`sig_date` AND ind.`code` = e.`code`
        WHERE e.`sig_date` = :d
        """
    )
    with repo.engine.begin() as conn:
        sig_df = pd.read_sql(stmt, conn, params={"d": latest_date})
    
    if sig_df.empty:
        logger.warning("筹码计算跳过：无信号数据。")
        return

    # 去重：不同策略可能选中同一只股票，筹码分只需计算一次
    sig_df_unique = sig_df.drop_duplicates(subset=["code"]).copy()

    # 计算并写入
    chip = ChipFilter()
    # ChipFilter 会自动更新 strategy_chip_filter 表
    # 注意：ChipFilter 内部逻辑是按 (date, code) 唯一键更新的，不区分 strategy_code
    # 这意味着同一个标的如果被多个策略选中，其筹码分是一样的，这是合理的。
    result = chip.apply(sig_df_unique)
    logger.info("筹码计算完成：已更新 %s 条记录。", len(result))


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

        # --- 中间步骤：计算筹码因子 ---
        logger.info("\n=== 阶段 2: 计算筹码因子 ===")
        # 随便用一个 runner 里的 db 连接即可
        if runners:
            first_runner = next(iter(runners.values()))
            run_chip_filter_for_all(first_runner, strategies_to_run)

        # --- 第 2 轮：核心策略重新决策 (消费筹码分) ---
        # 目前只有 ma5_ma20_trend 需要这一步来调整 quality_score 和 final_cap
        logger.info("\n=== 阶段 3: 核心策略二次决策 ===")
        core_strategy = "ma5_ma20_trend"
        if core_strategy in runners:
            run_strategy_pass(runners[core_strategy], force, "Pass 2")
        
        logger.info("==============================================")
        logger.info("流水线 3: 所有策略执行完成！")
        logger.info("==============================================")

    except Exception as e:
        logger.exception("流水线 3 执行失败: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
