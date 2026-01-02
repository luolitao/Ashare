import sys
import os
import logging
import pandas as pd
from datetime import datetime

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.utils.logger import setup_logger
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.strategies.ma_wyckoff_model import MAWyckoffStrategy, ACTION_BUY_STRONG, ACTION_BUY_LIGHT, ACTION_REDUCE, ACTION_SELL
from scripts.run_chip_filter import main as run_chip_filter

def get_db_engine():
    """获取数据库连接引擎"""
    db_config = DatabaseConfig.from_env()
    writer = MySQLWriter(db_config)
    return writer.engine

def load_candidate_pool(engine):
    """
    从数据库加载候选股票池。
    尝试顺序：
    1. strategy_candidates (筹码或其它策略产生的精选池)
    2. a_share_universe (AshareApp 生成的成交额前 N 基础池)
    3. dim_stock_basic (全市场兜底，仅取前 50 用于测试)
    """
    logger = logging.getLogger("Pipeline_2_Strategy")
    
    # 1. 尝试精选池
    try:
        sql = """
        SELECT t.code, b.code_name as name 
        FROM strategy_candidates t
        LEFT JOIN dim_stock_basic b ON t.code = b.code
        WHERE t.asof_trade_date = (SELECT MAX(asof_trade_date) FROM strategy_candidates)
        """
        df = pd.read_sql(sql, engine)
        if not df.empty:
            logger.info(f"从 strategy_candidates 加载了 {len(df)} 只精选标的。")
            return df
    except Exception:
        pass

    # 2. 尝试基础池 (Universe)
    try:
        sql = """
        SELECT code, code_name as name 
        FROM a_share_universe 
        WHERE date = (SELECT MAX(date) FROM a_share_universe)
        """
        df = pd.read_sql(sql, engine)
        if not df.empty:
            logger.info(f"精选池为空，从 a_share_universe 加载了 {len(df)} 只基础标的。")
            return df
    except Exception:
        pass

    # 3. 全市场兜底
    logger.warning("精选池和基础池均为空，使用全市场前 50 只股票作为兜底测试。")
    sql = "SELECT code, code_name as name FROM dim_stock_basic LIMIT 50"
    return pd.read_sql(sql, engine)

def get_stock_data(engine, code, limit=300):
    """获取单只股票的历史数据"""
    # 适配表名 history_daily_kline 和字段名 (trade_date, code)
    # 确保按日期升序排列
    sql = f"""
    SELECT trade_date as date, open, high, low, close, volume 
    FROM history_daily_kline 
    WHERE code='{code}' 
    ORDER BY trade_date ASC 
    LIMIT {limit}
    """
    df = pd.read_sql(sql, engine)
    
    # 确保 date 列是 datetime 类型，方便后续处理
    if not df.empty and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        
    return df

def main():
    setup_logger()
    logger = logging.getLogger("Pipeline_2_Strategy")
    
    logger.info("==============================================")
    logger.info(f"开始执行流水线 2: 策略扫描与信号生成 - {datetime.now()}")
    logger.info("==============================================")

    engine = get_db_engine()
    
    try:
        # 1. 运行前置筛选 (Chip Filter)
        logger.info(">>> 步骤 1/2: 运行筹码筛选 (Chip Filter)...")
        # 注意：run_chip_filter 需要确保它将结果写入数据库，以便后续步骤读取
        run_chip_filter() 
        logger.info(">>> 步骤 1/2 完成。")

        # 2. 运行核心策略 (MA + Wyckoff)
        logger.info(">>> 步骤 2/2: 运行 MA + Wyckoff 融合策略...")
        
        # 加载候选池
        candidates = load_candidate_pool(engine)
        logger.info(f"加载候选股票: {len(candidates)} 只")
        
        strategy = MAWyckoffStrategy(
            ma_short=5, 
            ma_long=20, 
            efi_window=60, 
            divergence_lookback=30,
            confirmation_window=10
        )
        
        results = []
        
        for idx, row in candidates.iterrows():
            code = row['code']
            name = row.get('name', code)
            
            # 获取数据
            try:
                df_price = get_stock_data(engine, code)
            except Exception as e:
                logger.warning(f"获取 {code} 数据失败: {e}")
                continue

            if len(df_price) < 100:
                continue
                
            # 运行策略
            df_res = strategy.run(df_price)
            
            # 获取最新一天的信号
            latest = df_res.iloc[-1]
            action = latest['action']
            
            # 如果有动作，记录下来
            if action != "HOLD":
                results.append({
                    'date': latest['date'],
                    'code': code,
                    'name': name,
                    'action': action,
                    'close': latest['close'],
                    'ma5': latest['ma_short'],
                    'ma20': latest['ma_long'],
                    'efi_z': latest['efi_z'],
                    'bull_div': latest['bullish_divergence'],
                    'bear_div': latest['bearish_divergence']
                })
                logger.info(f"[{code} {name}] 信号: {action}")

        # 3. 保存策略结果
        if results:
            df_results = pd.DataFrame(results)
            logger.info("\n====== 策略扫描结果 ======")
            logger.info(df_results[['code', 'name', 'action', 'close', 'efi_z']])
            
            # 写入数据库表 'strategy_signals'
            # df_results.to_sql('strategy_signals', engine, if_exists='append', index=False)
            
            # 同时也保存一个 CSV 方便查看（统一输出到 tool/output）
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            output_dir = os.path.join(project_root, "tool", "output")
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(
                output_dir,
                f"strategy_results_{datetime.now().strftime('%Y%m%d')}.csv",
            )
            df_results.to_csv(output_file, index=False)
            logger.info(f"结果已保存至文件: {output_file}")
            
        else:
            logger.info("今日无特殊交易信号。")

        logger.info("==============================================")
        logger.info("流水线 2 执行成功！")
        logger.info("==============================================")

    except Exception as e:
        logger.error(f"流水线 2 执行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
