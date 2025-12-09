"""实时行情与交易标的筛选工具."""

from __future__ import annotations

from typing import Set

import pandas as pd
import akshare as ak


class AshareUniverseBuilder:
    """基于 AKShare 实时行情构建当日交易标的候选池."""

    def __init__(self, top_liquidity_count: int = 100):
        self.top_liquidity_count = top_liquidity_count

    def _fetch_spot(self) -> pd.DataFrame:
        try:
            spot_df = ak.stock_zh_a_spot()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("获取全市场实时行情失败, 请检查网络或数据源可用性。") from exc
        if spot_df is None or spot_df.empty:
            raise RuntimeError("实时行情数据为空, 请稍后重试或更换数据源。")
        return spot_df

    def _fetch_st_codes(self, spot_df: pd.DataFrame) -> Set[str]:
        if "名称" not in spot_df.columns:
            return set()
        st_mask = spot_df["名称"].astype(str).str.contains("ST", case=False, na=False)
        return set(spot_df.loc[st_mask, "代码"].tolist())

    def _fetch_stop_codes(self, spot_df: pd.DataFrame) -> Set[str]:
        code_column = "代码"
        if code_column not in spot_df.columns:
            return set()

        if {"成交额", "成交量"}.issubset(spot_df.columns):
            stop_mask = (spot_df["成交额"] == 0) & (spot_df["成交量"] == 0)
        elif "成交量" in spot_df.columns:
            stop_mask = spot_df["成交量"] == 0
        else:
            return set()

        return set(spot_df.loc[stop_mask, code_column].tolist())

    def _fetch_new_stock_codes(self) -> Set[str]:
        fetchers = (ak.stock_zh_a_new, ak.stock_zh_a_new_df, ak.stock_zh_a_new_em)
        for fetcher in fetchers:
            try:
                new_df = fetcher()
                return set(new_df.get("代码", []))
            except Exception:  # noqa: BLE001
                continue
        return set()

    def build_universe(self) -> pd.DataFrame:
        """生成剔除 ST 与停牌标的后的实时行情清单."""

        spot_df = self._fetch_spot()
        st_codes = self._fetch_st_codes(spot_df)
        stop_codes = self._fetch_stop_codes(spot_df)
        new_codes = self._fetch_new_stock_codes()

        bad_codes = st_codes | stop_codes
        filtered = spot_df[~spot_df["代码"].isin(bad_codes)].copy()
        filtered["是否次新股"] = filtered["代码"].isin(new_codes)
        return filtered

    def pick_top_liquidity(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        """从候选池中筛选成交额最高的标的."""

        if "成交额" not in universe_df.columns:
            raise RuntimeError("候选池缺少成交额字段, 无法进行成交额排序。")
        sorted_df = universe_df.sort_values("成交额", ascending=False)
        return sorted_df.head(self.top_liquidity_count)
