"""输出指数周线通道情景（线性回归通道 + 30/60 周均线）。

用法：
  python run_index_weekly_channel.py [--include-current-week]

说明：
  - 从数据库 history_index_daily_kline 读取配置里的指数日线数据
  - 聚合成周线后，默认只输出最近一个“已收盘周”（避免周内未来日期）；
    若指定 --include-current-week，则会包含当前形成中的周线
  - 会调用开盘监测的环境构建逻辑，计算周线计划并写入环境快照表，供 open_monitor 复用
"""

from __future__ import annotations

import argparse
import datetime as dt
import json

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.baostock_core import BaostockDataFetcher
from ashare.baostock_session import BaostockSession
from ashare.config import get_section
from ashare.db import DatabaseConfig, MySQLWriter
from ashare.open_monitor import MA5MA20OpenMonitorRunner


def _parse_date(val: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:  # noqa: BLE001
        return None


def _load_trading_calendar(start: dt.date, end: dt.date) -> set[str]:
    try:
        client = BaostockDataFetcher(BaostockSession())
        calendar_df = client.get_trade_calendar(start.isoformat(), end.isoformat())
    except Exception:  # noqa: BLE001
        return set()

    if "is_trading_day" in calendar_df.columns:
        calendar_df = calendar_df[calendar_df["is_trading_day"].astype(str) == "1"]

    dates = (
        pd.to_datetime(calendar_df["calendar_date"], errors="coerce").dt.date.dropna().tolist()
    )
    return {d.isoformat() for d in dates}


def _resolve_latest_closed_week_end(latest_trade_date: str) -> tuple[str, bool]:
    trade_date = _parse_date(latest_trade_date)
    if trade_date is None:
        return latest_trade_date, True

    week_start = trade_date - dt.timedelta(days=trade_date.weekday())
    week_end = week_start + dt.timedelta(days=6)
    calendar = _load_trading_calendar(week_start - dt.timedelta(days=21), week_end)

    if calendar:
        last_trade_day = None
        for i in range(7):
            candidate = week_end - dt.timedelta(days=i)
            if candidate.isoformat() in calendar:
                last_trade_day = candidate
                break

        if last_trade_day:
            if trade_date == last_trade_day:
                return trade_date.isoformat(), True

            prev_candidate = week_start - dt.timedelta(days=1)
            for _ in range(30):
                if prev_candidate.isoformat() in calendar:
                    return prev_candidate.isoformat(), False
                prev_candidate -= dt.timedelta(days=1)

    fallback_friday = week_start + dt.timedelta(days=4)
    if trade_date >= fallback_friday:
        return fallback_friday.isoformat(), trade_date == fallback_friday

    prev_friday = fallback_friday - dt.timedelta(days=7)
    return prev_friday.isoformat(), False


def main() -> None:
    parser = argparse.ArgumentParser(description="输出指数周线通道情景")
    parser.add_argument(
        "--include-current-week",
        action="store_true",
        dest="include_current_week",
        help="包含当前形成中的周线（默认只输出最近已收盘周）",
    )
    args = parser.parse_args()

    db = MySQLWriter(DatabaseConfig.from_env())
    app_cfg = get_section("app") or {}
    codes = []
    if isinstance(app_cfg, dict):
        raw = app_cfg.get("index_codes", [])
        if isinstance(raw, (list, tuple)):
            codes = [str(c).strip() for c in raw if str(c).strip()]

    if not codes:
        print("config.yaml 未配置 app.index_codes，已跳过。")
        return

    stmt_latest = (
        text(
            """
            SELECT `code`, MAX(`date`) AS latest_date
            FROM history_index_daily_kline
            WHERE `code` IN :codes
            GROUP BY `code`
            """
        )
        .bindparams(bindparam("codes", expanding=True))
    )

    with db.engine.begin() as conn:
        latest_date_df = pd.read_sql_query(stmt_latest, conn, params={"codes": codes})

    if latest_date_df.empty:
        print("history_index_daily_kline 为空或未找到指定指数。")
        return

    latest_per_code = pd.to_datetime(latest_date_df["latest_date"], errors="coerce").dt.date.dropna()
    if latest_per_code.empty:
        print("history_index_daily_kline 为空或未找到指定指数。")
        return

    latest_date_val = min(latest_per_code)
    latest_date_str = pd.to_datetime(latest_date_val).date().isoformat()

    week_end_asof, _ = _resolve_latest_closed_week_end(latest_date_str)
    asof_date = latest_date_str if args.include_current_week else week_end_asof
    _, current_week_closed_asof = _resolve_latest_closed_week_end(asof_date)

    runner = MA5MA20OpenMonitorRunner()
    checked_at = dt.datetime.now()
    monitor_date = checked_at.date().isoformat()
    dedupe_bucket = f"daily_{asof_date}"
    env_context = runner.build_and_persist_env_snapshot(
        asof_date,
        monitor_date=monitor_date,
        dedupe_bucket=dedupe_bucket,
        checked_at=checked_at,
    )

    output = {"env_snapshot": env_context or {}, "asof_trade_date": asof_date}
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
