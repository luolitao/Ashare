import sys
import os
import pandas as pd
from sqlalchemy import text

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.config import load_config
from ashare.core.db import DatabaseConfig, MySQLWriter

def audit_data_quality():
    print("=== AShare 数据质量深度审计 ===")
    try:
        db = MySQLWriter(DatabaseConfig.from_env())
        with db.engine.connect() as conn:
            # 1. 检查 K 线与指标的同步性
            k_cnt = conn.execute(text("SELECT COUNT(*) FROM history_daily_kline")).scalar()
            i_cnt = conn.execute(text("SELECT COUNT(*) FROM strategy_ind_daily")).scalar()
            print(f"[KV] 记录分布: DailyK={k_cnt}, Indicators={i_cnt}")
            if abs(k_cnt - i_cnt) / max(1, k_cnt) > 0.1:
                print("[WARN] K线与指标数量差异较大，可能存在计算遗漏！")

            # 2. 检查成交量量纲异常 (纠正后的逻辑)
            # 抽样对比日线成交量与 5 分钟成交量
            print("\n--- 成交量量纲审计 ---")
            vol_check_sql = """
                SELECT k.code, k.date, k.volume as daily_vol, m.volume as min_vol 
                FROM history_daily_kline k
                JOIN strategy_mon_minute m ON k.code = m.code AND k.date = m.monitor_date
                WHERE k.date = (SELECT MAX(date) FROM history_daily_kline)
                LIMIT 5
            """
            vols = pd.read_sql(text(vol_check_sql), conn)
            if not vols.empty:
                for _, row in vols.iterrows():
                    ratio = row['daily_vol'] / max(1, row['min_vol'])
                    # 对于 5 分钟数据，日线量通常应大于分时量 10-50 倍左右。
                    # 如果比例接近 1 或 0.01，说明单位有问题
                    status = "[OK]" if ratio > 1 else "[ERROR: Unit Mismatch? у]"
                    print(f"[{row['code']}] Daily: {row['daily_vol']}, Minute: {row['min_vol']}, Ratio: {ratio:.2f} {status}")
            else:
                print("[SKIP] 缺少分时数据，无法比对量纲。")

            # 3. 检查计算窗口预热
            print("\n--- 计算窗口审计 ---")
            window_sql = "SELECT code, COUNT(*) as cnt FROM history_daily_kline GROUP BY code LIMIT 5"
            windows = pd.read_sql(text(window_sql), conn)
            for _, row in windows.iterrows():
                status = "[OK]" if row['cnt'] >= 250 else "[WARN: Window too short for MA250]"
                print(f"[{row['code']}] History Depth: {row['cnt']} days {status}")

    except Exception as e:
        print(f"[CRITICAL] 审计中断: {e}")

if __name__ == "__main__":
    audit_data_quality()