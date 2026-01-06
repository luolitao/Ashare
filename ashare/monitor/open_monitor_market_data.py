"""open_monitor 行情抓取与路由层。"""

from __future__ import annotations

from typing import Any, List
import concurrent.futures
import time

import pandas as pd

from ashare.monitor.open_monitor_quotes import (
    fetch_minute_eastmoney,
    fetch_quotes_akshare,
    fetch_quotes_eastmoney,
)
from ashare.data.akshare_fetcher import AkshareDataFetcher


class OpenMonitorMarketData:
    """开盘监测行情抓取与数据源路由。"""

    def __init__(self, logger, params) -> None:
        self.logger = logger
        self.params = params
        self._ak_fetcher = None

    @property
    def ak_fetcher(self) -> AkshareDataFetcher:
        if self._ak_fetcher is None:
            self._ak_fetcher = AkshareDataFetcher()
        return self._ak_fetcher

    def fetch_quotes(self, codes: List[str]) -> pd.DataFrame:
        """获取实时行情。

        A 修复点：
        - live_trade_date 优先使用行情源字段（trade_date/date/live_trade_date）。
        - 若行情源未提供交易日字段，使用本次运行的 monitor_date（或 checked_at.date）兜底，便于对账。
        - 增加脏数据检测与重试逻辑（针对 VWAP 超出 High/Low 范围的情况）。
        """
        if not codes:
            return pd.DataFrame(columns=["code"])

        source = (self.params.quote_source or "eastmoney").strip().lower()
        
        def _fetch_batch(batch_codes: List[str]) -> pd.DataFrame:
            if source == "akshare":
                return self._fetch_quotes_akshare(batch_codes)
            else:
                return self._fetch_quotes_eastmoney(batch_codes)

        # 1. 首次全量拉取
        df = _fetch_batch(codes)

        # 2. 脏数据检测与重试 (VWAP Sanity Check)
        # 场景：Amount 和 Volume 更新不同步，导致 VWAP > High 或 VWAP < Low
        if not df.empty and {"live_amount", "live_volume", "live_high", "live_low"}.issubset(df.columns):
            from ashare.utils.convert import to_float as _to_float
            
            dirty_codes = []
            for _, row in df.iterrows():
                amt = _to_float(row.get("live_amount"))
                vol = _to_float(row.get("live_volume"))
                high = _to_float(row.get("live_high"))
                low = _to_float(row.get("live_low"))
                
                if amt and vol and vol > 0:
                    vwap = amt / vol
                    # 容差 0.1% 处理浮点微小误差
                    tolerance = 0.001
                    is_dirty = False
                    
                    if high and vwap > high * (1 + tolerance):
                        is_dirty = True
                    elif low and vwap < low * (1 - tolerance):
                        is_dirty = True
                    
                    if is_dirty:
                        code = str(row.get("code"))
                        dirty_codes.append(code)
                        self.logger.warning(
                            "检测到脏行情数据: code=%s, vwap=%.3f, high=%.3f, low=%.3f. 触发重试。",
                            code, vwap, high or 0, low or 0
                        )

            if dirty_codes:
                # 短暂休眠让数据源同步
                time.sleep(0.2)
                self.logger.info("开始重试获取 %d 只脏数据标的...", len(dirty_codes))
                
                retry_df = _fetch_batch(dirty_codes)
                
                if not retry_df.empty:
                    # 从原 df 中剔除脏数据行
                    df = df[~df["code"].isin(dirty_codes)].copy()
                    # 合并重试后的数据
                    df = pd.concat([df, retry_df], ignore_index=True)
                    self.logger.info("重试完成，已合并 %d 条新数据。", len(retry_df))

        # ---- A: 补齐 live_trade_date（优先使用行情源字段；缺失时兜底 monitor_date）----
        if "live_trade_date" not in df.columns:
            df["live_trade_date"] = pd.NA
        for cand in ("trade_date", "date"):
            if cand in df.columns:
                df["live_trade_date"] = df["live_trade_date"].fillna(df[cand])

        checked_at = getattr(self.params, "checked_at", None)
        monitor_date = getattr(self.params, "monitor_date", None)
        if monitor_date:
            df["live_trade_date"] = df["live_trade_date"].fillna(monitor_date)
        elif checked_at is not None:
            df["live_trade_date"] = df["live_trade_date"].fillna(
                checked_at.date().isoformat()
            )

        if checked_at is not None:
            df["quote_fetched_at"] = checked_at
            df["quote_fetched_date"] = checked_at.date().isoformat()

        return df

    def fetch_index_live_quote(self) -> dict[str, Any]:
        code = str(self.params.index_code or "").strip()
        if not code:
            return {}
        df = self.fetch_quotes([code])
        if df.empty:
            return {"index_code": code}
        row = df.iloc[0].to_dict()
        row["index_code"] = code

        live_trade_date = row.get("live_trade_date")
        if pd.isna(live_trade_date):
            live_trade_date = None
        if not live_trade_date:
            live_trade_date = row.get("trade_date") or row.get("date")
        if pd.isna(live_trade_date):
            live_trade_date = None
        if not live_trade_date:
            monitor_date = getattr(self.params, "monitor_date", None)
            checked_at = getattr(self.params, "checked_at", None)
            if monitor_date:
                live_trade_date = monitor_date
            elif checked_at is not None:
                live_trade_date = checked_at.date().isoformat()
        row["live_trade_date"] = live_trade_date
        return row

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_akshare(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        strict_quotes = bool(getattr(self.params, "strict_quotes", True))
        return fetch_quotes_eastmoney(codes, strict_quotes=strict_quotes, logger=self.logger)

    def _fetch_minute_data_raw(self, code: str, trade_date: str | None) -> pd.DataFrame:
        source = (self.params.quote_source or "eastmoney").strip().lower()
        if source == "eastmoney":
            return fetch_minute_eastmoney(code, trade_date=trade_date, logger=self.logger)
        return self.ak_fetcher.fetch_minute_data(code, trade_date=trade_date)

    def fetch_minute_data(
        self,
        code: str,
        *,
        trade_date: str | None = None,
        timeout_sec: float | None = None,
    ) -> pd.DataFrame:
        """获取单只股票的分时数据（用于低吸判断）。"""
        try:
            if timeout_sec is None or timeout_sec <= 0:
                return self._fetch_minute_data_raw(code, trade_date)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self._fetch_minute_data_raw, code, trade_date)
                return future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            self.logger.warning("Fetch minute data timeout for %s (%.1fs)", code, timeout_sec)
            return pd.DataFrame()
        except Exception as e:
            self.logger.warning(f"Failed to fetch minute data for {code}: {e}")
            return pd.DataFrame()
