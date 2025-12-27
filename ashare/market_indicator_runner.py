from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text

from .market_indicator_builder import MarketIndicatorBuilder
from .open_monitor_repo import OpenMonitorRepository


class MarketIndicatorRunner:
    """日线/周线指标回填入口。"""

    def __init__(
        self,
        *,
        repo: OpenMonitorRepository,
        builder: MarketIndicatorBuilder,
        logger: logging.Logger,
    ) -> None:
        self.repo = repo
        self.builder = builder
        self.logger = logger

    def run_weekly_indicator(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        index_code = str(self.repo.params.index_code or "").strip()
        end_dt = self._resolve_end_date(end_date, index_code)
        if end_dt is None:
            raise ValueError("无法解析 weekly 指标的 end_date。")

        start_dt = self._resolve_start_date(
            start_date,
            end_dt,
            mode=mode,
            latest_date=self.repo.get_latest_weekly_indicator_date(index_code),
        )
        weekly_dates = self.builder.resolve_weekly_asof_dates(start_dt, end_dt)
        if not weekly_dates:
            return {"written": 0, "start_date": start_dt.isoformat(), "end_date": end_dt.isoformat()}

        written = 0
        for asof_date in weekly_dates:
            rows = self.builder.compute_weekly_indicator(
                asof_date.isoformat(), checked_at=dt.datetime.now()
            )
            written += self.repo.upsert_weekly_indicator(rows)
        return {
            "written": written,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
        }

    def run_daily_indicator(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        index_code = str(self.repo.params.index_code or "").strip()
        end_dt = self._resolve_end_date(end_date, index_code)
        if end_dt is None:
            raise ValueError("无法解析 daily 指标的 end_date。")

        start_dt = self._resolve_start_date(
            start_date,
            end_dt,
            mode=mode,
            latest_date=self.repo.get_latest_daily_indicator_date(index_code),
        )
        rows = self.builder.compute_daily_indicators(start_dt, end_dt)
        written = self.repo.upsert_daily_indicator(rows)
        return {
            "written": written,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
        }

    def _resolve_end_date(
        self, end_date: Optional[str], index_code: str
    ) -> dt.date | None:
        if end_date:
            try:
                return dt.date.fromisoformat(str(end_date))
            except Exception:
                return None
        stmt = text(
            """
            SELECT MAX(`date`) AS latest_date
            FROM history_index_daily_kline
            WHERE `code` = :code
            """
        )
        try:
            with self.repo.engine.begin() as conn:
                row = conn.execute(stmt, {"code": index_code}).mappings().first()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("读取最新交易日失败：%s", exc)
            return None
        if not row:
            return None
        latest = row.get("latest_date")
        if isinstance(latest, dt.datetime):
            return latest.date()
        if isinstance(latest, dt.date):
            return latest
        if latest:
            try:
                return pd.to_datetime(latest).date()
            except Exception:
                return None
        return None

    def _resolve_start_date(
        self,
        start_date: Optional[str],
        end_dt: dt.date,
        *,
        mode: str,
        latest_date: Optional[dt.date],
    ) -> dt.date:
        if start_date:
            return dt.date.fromisoformat(str(start_date))
        mode_norm = str(mode or "").strip().lower() or "incremental"
        if mode_norm == "incremental" and latest_date:
            return latest_date + dt.timedelta(days=1)
        default_days = 365
        return end_dt - dt.timedelta(days=default_days)
