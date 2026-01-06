
import sys
import os
import json
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text
from dataclasses import dataclass, asdict

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.db import DatabaseConfig

@dataclass
class StockContext:
    code: str
    name: str | None = None
    industry: str | None = None
    latest_signal: dict | None = None
    monitor_status: dict | None = None
    market_snapshot: dict | None = None
    basic_info: dict | None = None

class StockInspector:
    def __init__(self):
        self.db_config = DatabaseConfig.from_env()
        self.engine = create_engine(
            self.db_config.database_url(),
            echo=False,
            pool_recycle=3600,
            pool_pre_ping=True
        )

    def inspect(self, code: str) -> StockContext:
        ctx = StockContext(code=code)
        
        with self.engine.connect() as conn:
            # 1. 基本信息 & 行业
            basic_query = text("""
                SELECT b.code_name, i.industry 
                FROM dim_stock_basic b
                LEFT JOIN dim_stock_industry i ON b.code = i.code
                WHERE b.code = :code
            """)
            basic = conn.execute(basic_query, {"code": code}).mappings().first()
            if basic:
                ctx.name = basic.get("code_name")
                ctx.industry = basic.get("industry")
                ctx.basic_info = dict(basic)

            # 2. 最新信号 (Strategy Signals)
            # 查询 v_strategy_sig_ready
            sig_query = text("""
                SELECT sig_date, strategy_code, `signal`, reason, risk_tag
                FROM v_strategy_sig_ready
                WHERE code = :code
                ORDER BY sig_date DESC
                LIMIT 1
            """)
            sig = conn.execute(sig_query, {"code": code}).mappings().first()
            if sig:
                # Handle date serialization
                res = dict(sig)
                if isinstance(res.get("sig_date"), (datetime,)):
                    res["sig_date"] = res["sig_date"].strftime("%Y-%m-%d")
                ctx.latest_signal = res

            # 3. 实盘监控 (Open Monitor)
            # 查询 v_monitor_simple
            mon_query = text("""
                SELECT monitor_date, action, action_reason, live_vwap, pct, vol_ratio, strategy_code
                FROM v_monitor_simple
                WHERE code = :code
                ORDER BY run_pk DESC
                LIMIT 1
            """)
            mon = conn.execute(mon_query, {"code": code}).mappings().first()
            if mon:
                res = dict(mon)
                if isinstance(res.get("monitor_date"), (datetime,)):
                    res["monitor_date"] = res["monitor_date"].strftime("%Y-%m-%d")
                ctx.monitor_status = res

            # 4. 行情快照 (Quotes)
            # 查询 strategy_mon_quotes
            quote_query = text("""
                SELECT live_trade_date, live_open, live_high, live_low, live_latest, live_volume, live_amount
                FROM strategy_mon_quotes
                WHERE code = :code
                ORDER BY run_pk DESC
                LIMIT 1
            """)
            quote = conn.execute(quote_query, {"code": code}).mappings().first()
            if quote:
                res = dict(quote)
                if isinstance(res.get("live_trade_date"), (datetime,)):
                    res["live_trade_date"] = res["live_trade_date"].strftime("%Y-%m-%d")
                ctx.market_snapshot = res

        return ctx

    def to_json(self, ctx: StockContext) -> str:
        return json.dumps(asdict(ctx), ensure_ascii=False, indent=2)

    def to_markdown(self, ctx: StockContext) -> str:
        lines = []
        name = ctx.name or "Unknown"
        lines.append(f"# 🕵️‍♂️ 股票侦探报告: {name} ({ctx.code})")
        
        if ctx.industry:
            lines.append(f"**行业**: {ctx.industry}")
        
        lines.append("---")
        
        # 1. 监控状态 (最重要)
        mon = ctx.monitor_status
        if mon:
            icon = "🟢" if mon.get("action") == "EXECUTE" else "🔴" if mon.get("action") == "STOP" else "🟡"
            lines.append(f"## {icon} 实盘监控 ({mon.get('monitor_date')})")
            lines.append(f"- **决策**: **{mon.get('action')}**")
            lines.append(f"- **理由**: {mon.get('action_reason')}")
            lines.append(f"- **均价 (VWAP)**: {mon.get('live_vwap'):.3f}")
            lines.append(f"- **涨幅**: {mon.get('pct'):.2f}%")
            lines.append(f"- **量比**: {mon.get('vol_ratio'):.2f}")
        else:
            lines.append("## ⚪ 实盘监控: 无数据")

        # 2. 信号源头
        sig = ctx.latest_signal
        if sig:
            lines.append(f"\n## 📡 信号源头 ({sig.get('sig_date')})")
            lines.append(f"- **策略**: `{sig.get('strategy_code')}`")
            lines.append(f"- **信号**: `{sig.get('signal')}`")
            lines.append(f"- **原因**: {sig.get('reason')}")
            if sig.get('risk_tag'):
                lines.append(f"- **风险标签**: `{sig.get('risk_tag')}`")
        else:
            lines.append("\n## 📡 信号源头: 无历史信号")

        # 3. 行情快照
        q = ctx.market_snapshot
        if q:
            lines.append(f"\n## 📊 行情快照")
            lines.append(f"- **现价**: {q.get('live_latest')}")
            lines.append(f"- **最高**: {q.get('live_high')}")
            lines.append(f"- **最低**: {q.get('live_low')}")
            lines.append(f"- **成交额**: {float(q.get('live_amount') or 0) / 100000000:.2f} 亿")

        return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Stock Inspector")
    parser.add_argument("code", help="Stock code (e.g., sz.002261)")
    parser.add_argument("--json", action="store_true", help="Output JSON format")
    args = parser.parse_args()

    inspector = StockInspector()
    ctx = inspector.inspect(args.code)

    if args.json:
        print(inspector.to_json(ctx))
    else:
        print(inspector.to_markdown(ctx))

if __name__ == "__main__":
    main()
