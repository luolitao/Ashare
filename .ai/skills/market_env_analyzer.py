"""
Market Environment Analyzer Skill (Optimized)
Usage: python .ai/skills/market_env_analyzer.py [date]
"""
import sys
import os
import logging
import json
import datetime as dt

# Fix path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.db import MySQLWriter, DatabaseConfig
from ashare.indicators.weekly_env_builder import WeeklyEnvironmentBuilder

def analyze_market_env(target_date: str = None):
    db = MySQLWriter(DatabaseConfig.from_env())
    if not target_date:
        target_date = dt.date.today().isoformat()
    
    builder = WeeklyEnvironmentBuilder(db_writer=db, logger=logging.getLogger("Skill"), index_codes=["sh.000001"])
    try:
        ctx = builder.build_environment_context(target_date)
    except Exception as e:
        print(f"Error: {e}")
        return

    # ASCII Visualization for Risk Level
    risk_val = ctx.get("weekly_risk_score", 50)
    risk_bar = "█" * int(risk_val / 5) + "░" * (20 - int(risk_val / 5))
    
    print("\n" + "="*40)
    print(f" MARKET ENVIRONMENT: {target_date}")
    print("="*40)
    print(f"REGIME     : {ctx.get('regime')}")
    print(f"RISK LEVEL : [{risk_bar}] {ctx.get('weekly_risk_level')} ({risk_val:.1f}%)")
    print(f"SCENE      : {ctx.get('weekly_scene_code')}")
    print(f"GATE ACTION: {ctx.get('env_final_gate_action')}")
    print(f"CAP LIMIT  : {ctx.get('env_final_cap_pct')*100:.1f}%")
    print("-