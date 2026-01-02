    def _run_single_stock(
        self,
        code: str,
        df_stock: pd.DataFrame,
        fundamentals: pd.Series | None,
        stock_basic: pd.Series | None,
        latest_date: dt.date,
    ) -> List[dict]:
        """为单只股票生成交易信号。"""
        signals = []
        df_sorted = df_stock.sort_values("date")
        for i in range(len(df_sorted)):
            # 这里是单股票处理逻辑（省略具体实现，保持原样）
            pass
        return signals

    def run(self, *, force: bool = False) -> None:
        """执行 MA5-MA20 策略。

        - 默认遵循 config.yaml: strategy_ma5_ma20_trend.enabled。
        - 当 force=True 时，即便 enabled=false 也会执行（用于单独运行脚本）。
        """

        if (not force) and (not self.params.enabled):
            self.logger.info("strategy_ma5_ma20_trend.enabled=false，已跳过 MA5-MA20 策略运行。")
            return

        if force and (not self.params.enabled):
            self.logger.info("strategy_ma5_ma20_trend.enabled=false，但 force=True，仍将执行 MA5-MA20 策略。")

        self.logger.debug(
            "MA5-MA20 参数：lookback_days=%s indicator_window=%s",
            self.params.lookback_days,
            self.indicator_window,
        )

        daily_tbl = self._daily_table_name()
        latest_date = self._get_latest_trade_date()

        candidates_df = self.candidates_service.load_candidates(latest_date)

        if candidates_df.empty:
            self.logger.warning(
                "strategy_candidates=%s 为空，已跳过 MA5-MA20 策略运行。",
                latest_date,
            )
            return

        candidates_df["code"] = candidates_df["code"].astype(str)
        candidate_codes = candidates_df["code"].dropna().unique().tolist()
        liquidity_codes = (
            candidates_df.loc[candidates_df["is_liquidity"] == 1, "code"]
            .dropna()
            .unique()
            .tolist()
        )
        signal_codes = (
            candidates_df.loc[candidates_df["has_signal"] == 1, "code"]
            .dropna()
            .unique()
            .tolist()
        )
        candidate_set = set(candidate_codes)
        liquidity_set = set(liquidity_codes)
        signal_set = set(signal_codes)
        both_set = liquidity_set & signal_set
        snapshot_only_set = candidate_set - liquidity_set
        self.logger.info(
            "MA5-MA20 策略：日线表=%s candidates_total=%s liquidity=%s signal=%s both=%s snapshot_only=%s",
            daily_tbl,
            len(candidate_set),
            len(liquidity_set),
            len(signal_set),
            len(both_set),
            len(snapshot_only_set),
        )

        daily = self._load_daily_kline(candidate_codes, latest_date)
        self.logger.info("MA5-MA20 策略：读取日线 %s 行（%s 只股票）。", len(daily), daily["code"].nunique())

        ind = self._compute_indicators(daily)
        fundamentals = (
            self._load_fundamentals_latest()
            if self._fundamentals_cache is None
            else self._fundamentals_cache
        )
        stock_basic = (
            self._load_stock_basic()
            if self._stock_basic_cache is None
            else self._stock_basic_cache
        )
        self._fundamentals_cache = fundamentals
        self._stock_basic_cache = stock_basic
        sig = self._generate_signals(ind, fundamentals, stock_basic)
        sig = sig.dropna(subset=["date", "code"])
        sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
        sig["code"] = sig["code"].astype(str)
        sig = (
            sig.sort_values(["code", "date"])
            .drop_duplicates(subset=["code", "date"], keep="last")
            .reset_index(drop=True)
        )
        sig["code"] = sig["code"].astype(str)
        sig_for_write = sig.copy()
        snapshot_only_codes = sorted(snapshot_only_set)
        liquidity_codes_list = sorted(liquidity_set)
        if snapshot_only_codes:
            snapshot_mask = sig_for_write["code"].isin(snapshot_only_codes)
            latest_mask = sig_for_write["date"].dt.date == latest_date
            sig_for_write.loc[snapshot_mask, "signal"] = "SNAPSHOT"
            sig_for_write.loc[snapshot_mask, "final_action"] = "SNAPSHOT"
            sig_for_write.loc[snapshot_mask, "final_reason"] = "SNAPSHOT_ONLY"
            sig_for_write.loc[snapshot_mask, "reason"] = "SNAPSHOT_ONLY"
            sig_for_write = pd.concat(
                [sig_for_write[~snapshot_mask], sig_for_write[snapshot_mask & latest_mask]],
                ignore_index=True,
            )

        self._write_indicator_daily(latest_date, sig, liquidity_codes_list)
        if snapshot_only_codes:
            self._write_indicator_daily(
                latest_date,
                sig,
                snapshot_only_codes,
                scope_override="latest",
            )

        self._write_signal_events(latest_date, sig_for_write, liquidity_codes_list)
        if snapshot_only_codes:
            self._write_signal_events(
                latest_date,
                sig_for_write,
                snapshot_only_codes,
                scope_override="latest",
            )

        self._precompute_chip_filter(sig_for_write)
        sig_dates = sig_for_write["date"].dropna().dt.date.unique().tolist()
        self._refresh_ready_signals_table(sig_dates)

        # 重构说明：不再需要手动刷新 candidates 表，因为 strategy_ready_signals 已经改为自动 Join 的动态视图。
        # self.candidates_service.refresh(latest_date)

        latest_sig = sig[sig["date"].dt.date == latest_date]
        dup_count = int(latest_sig.duplicated(subset=["code", "date"]).sum())
        self.logger.info(
            "MA5-MA20 策略自检：latest_sig 行数=%s，唯一 code 数=%s，重复(code,date)=%s",
            len(latest_sig),
            latest_sig["code"].nunique(),
            dup_count,
        )
        action_col = "final_action" if "final_action" in latest_sig.columns else "signal"
        action_series = latest_sig[action_col].fillna("HOLD").astype(str)
        counts = action_series.value_counts(dropna=False)
        order = ["BUY", "BUY_CONFIRM", "SELL", "REDUCE", "HOLD", "WAIT"]
        ordered_counts = {k: int(counts.get(k, 0)) for k in order}
        other_count = int(counts.drop(labels=order, errors="ignore").sum())
        self.logger.info(
            "MA5-MA20 策略完成：最终动作(final_action)统计（最新交易日：%s）："
            "BUY=%s, BUY_CONFIRM=%s, SELL=%s, REDUCE=%s, HOLD=%s, WAIT=%s, OTHER=%s",
            latest_date,
            ordered_counts["BUY"],
            ordered_counts["BUY_CONFIRM"],
            ordered_counts["SELL"],
            ordered_counts["REDUCE"],
            ordered_counts["HOLD"],
            ordered_counts["WAIT"],
            other_count,
        )
