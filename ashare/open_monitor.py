"""开盘监测：检查“前一交易日收盘信号”在今日开盘是否仍可执行。

目标：
- 读取 strategy_ma5_ma20_signals 中“最新交易日”的 BUY 信号（通常是昨天收盘跑出来的）。
- 在开盘/集合竞价阶段拉取实时行情（今开/最新价），做二次过滤：
  - 高开过多（追高风险/买不到合理价）
  - 低开破位（跌破 MA20 / 大幅低开）
  - 涨停（大概率买不到）

输出：
- 可选写入 MySQL：strategy_ma5_ma20_open_monitor（默认 append）
- 可选导出 CSV 到 output/open_monitor

注意：
- 该脚本“只做监测与清单输出”，不下单。
- 实时行情优先用 AkShare 的东方财富 A 股实时接口；若未安装 AkShare，则自动回退到 Eastmoney push2 接口。
"""

from __future__ import annotations

import datetime as dt
import json
import math
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import bindparam, text

from .config import get_section
from .db import DatabaseConfig, MySQLWriter
from .utils.logger import setup_logger


def _to_float(value: Any) -> float | None:  # noqa: ANN401
    try:
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            if v in {"", "-", "--", "None", "nan"}:
                return None
            return float(v.replace(",", ""))
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            return float(value)
        return float(value)
    except Exception:
        return None


def _strip_baostock_prefix(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh.") or code.startswith("sz."):
        return code[3:]
    return code


def _to_baostock_code(exchange: str, symbol: str) -> str:
    ex = str(exchange or "").lower().strip()
    sym = str(symbol or "").strip()
    if ex in {"sh", "1"}:
        return f"sh.{sym}"
    if ex in {"sz", "0"}:
        return f"sz.{sym}"
    # fallback：猜测 6/9 为沪，0/3 为深
    if sym.startswith(("6", "9")):
        return f"sh.{sym}"
    return f"sz.{sym}"


def _to_eastmoney_secid(code: str) -> str:
    code = str(code or "").strip()
    if code.startswith("sh."):
        return f"1.{code[3:]}"
    if code.startswith("sz."):
        return f"0.{code[3:]}"
    # 尝试裸代码
    digits = _strip_baostock_prefix(code)
    if digits.startswith(("6", "9")):
        return f"1.{digits}"
    return f"0.{digits}"


@dataclass(frozen=True)
class OpenMonitorParams:
    """开盘监测参数（支持从 config.yaml 的 open_monitor 覆盖）。"""

    enabled: bool = True

    # 信号来源表：默认沿用 MA5-MA20 策略 signals_table
    signals_table: str = "strategy_ma5_ma20_signals"

    # 输出表：开盘检查结果
    output_table: str = "strategy_ma5_ma20_open_monitor"

    # 行情来源：auto / akshare / eastmoney
    quote_source: str = "auto"

    # 核心过滤规则
    max_gap_up_pct: float = 0.05       # 今开相对昨收涨幅 > 5% → skip
    max_gap_down_pct: float = -0.03    # 今开相对昨收跌幅 < -3% → skip
    min_open_vs_ma20_pct: float = 0.0  # 今开 < MA20*(1+min_open_vs_ma20_pct) → skip（默认需站上 MA20）
    limit_up_trigger_pct: float = 9.7  # 涨跌幅 >= 9.7% → 视为涨停/接近涨停，skip

    # 输出控制
    write_to_db: bool = True
    export_csv: bool = True
    export_top_n: int = 100
    output_subdir: str = "open_monitor"

    @classmethod
    def from_config(cls) -> "OpenMonitorParams":
        sec = get_section("open_monitor") or {}
        if not isinstance(sec, dict):
            sec = {}

        # 默认 signals_table 与策略保持一致
        strat = get_section("strategy_ma5_ma20_trend") or {}
        if isinstance(strat, dict):
            default_signals = strat.get("signals_table", cls.signals_table)
        else:
            default_signals = cls.signals_table

        def _get_bool(key: str, default: bool) -> bool:
            val = sec.get(key, default)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(val)

        def _get_float(key: str, default: float) -> float:
            raw = sec.get(key, default)
            parsed = _to_float(raw)
            return default if parsed is None else float(parsed)

        def _get_int(key: str, default: int) -> int:
            raw = sec.get(key, default)
            try:
                return int(raw)
            except Exception:
                return default

        quote_source = str(sec.get("quote_source", cls.quote_source)).strip().lower() or "auto"

        return cls(
            enabled=_get_bool("enabled", cls.enabled),
            signals_table=str(sec.get("signals_table", default_signals)).strip() or default_signals,
            output_table=str(sec.get("output_table", cls.output_table)).strip() or cls.output_table,
            quote_source=quote_source,
            max_gap_up_pct=_get_float("max_gap_up_pct", cls.max_gap_up_pct),
            max_gap_down_pct=_get_float("max_gap_down_pct", cls.max_gap_down_pct),
            min_open_vs_ma20_pct=_get_float("min_open_vs_ma20_pct", cls.min_open_vs_ma20_pct),
            limit_up_trigger_pct=_get_float("limit_up_trigger_pct", cls.limit_up_trigger_pct),
            write_to_db=_get_bool("write_to_db", cls.write_to_db),
            export_csv=_get_bool("export_csv", cls.export_csv),
            export_top_n=_get_int("export_top_n", cls.export_top_n),
            output_subdir=str(sec.get("output_subdir", cls.output_subdir)).strip() or cls.output_subdir,
        )


class MA5MA20OpenMonitorRunner:
    """开盘监测 Runner：读取前一交易日 BUY 信号 → 拉实时行情 → 输出可执行清单。"""

    def __init__(self) -> None:
        self.logger = setup_logger()
        self.params = OpenMonitorParams.from_config()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())

    # -------------------------
    # DB helpers
    # -------------------------
    def _table_exists(self, table: str) -> bool:
        try:
            with self.db_writer.engine.begin() as conn:
                df = pd.read_sql_query(text("SHOW TABLES LIKE :t"), conn, params={"t": table})
            return not df.empty
        except Exception:
            return False

    def _load_latest_buy_signals(self) -> Tuple[str | None, pd.DataFrame]:
        table = self.params.signals_table
        with self.db_writer.engine.begin() as conn:
            try:
                max_df = pd.read_sql_query(
                    text(f"SELECT MAX(`date`) AS max_date FROM `{table}`"),
                    conn,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 signals_table=%s 失败：%s", table, exc)
                return None, pd.DataFrame()

        if max_df.empty:
            return None, pd.DataFrame()

        max_date = max_df.iloc[0].get("max_date")
        if pd.isna(max_date) or not str(max_date).strip():
            return None, pd.DataFrame()

        signal_date = str(max_date)[:10]
        self.logger.info("最新信号交易日：%s（来源表=%s）", signal_date, table)

        with self.db_writer.engine.begin() as conn:
            try:
                df = pd.read_sql_query(
                    text(
                        f"""
                        SELECT
                          `date`,`code`,`close`,
                          `ma5`,`ma20`,`ma60`,`ma250`,
                          `vol_ratio`,`macd_hist`,`kdj_k`,`kdj_d`,`atr14`,`stop_ref`,
                          `signal`,`reason`
                        FROM `{table}`
                        WHERE `date` = :d AND `signal` = 'BUY'
                        """
                    ),
                    conn,
                    params={"d": signal_date},
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error("读取 %s BUY 信号失败：%s", table, exc)
                return signal_date, pd.DataFrame()

        if df.empty:
            self.logger.info("%s 没有任何 BUY 信号，跳过开盘监测。", signal_date)
            return signal_date, df

        df["code"] = df["code"].astype(str)
        return signal_date, df

    # -------------------------
    # Quote fetch
    # -------------------------
    def _fetch_quotes(self, codes: List[str]) -> pd.DataFrame:
        source = (self.params.quote_source or "auto").lower()

        if source in {"auto", "akshare"}:
            df = self._fetch_quotes_akshare(codes)
            if not df.empty:
                return df
            if source == "akshare":
                return df

        return self._fetch_quotes_eastmoney(codes)

    def _fetch_quotes_akshare(self, codes: List[str]) -> pd.DataFrame:
        try:
            import akshare as ak  # type: ignore
        except Exception as exc:  # noqa: BLE001
            self.logger.info("AkShare 不可用（将回退）：%s", exc)
            return pd.DataFrame()

        digits = {_strip_baostock_prefix(c) for c in codes}
        try:
            spot = ak.stock_zh_a_spot_em()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AkShare 行情拉取失败（将回退）：%s", exc)
            return pd.DataFrame()

        if spot is None or getattr(spot, "empty", True):
            return pd.DataFrame()

        rename_map = {
            "代码": "symbol",
            "名称": "name",
            "最新价": "latest",
            "涨跌幅": "pct_change",
            "今开": "open",
            "昨收": "prev_close",
        }
        for k in list(rename_map.keys()):
            if k not in spot.columns:
                rename_map.pop(k, None)

        spot = spot.rename(columns=rename_map)
        if "symbol" not in spot.columns:
            return pd.DataFrame()

        spot["symbol"] = spot["symbol"].astype(str)
        spot = spot[spot["symbol"].isin(digits)].copy()
        if spot.empty:
            return pd.DataFrame()

        out = pd.DataFrame()
        out["code"] = spot["symbol"].apply(lambda x: _to_baostock_code("auto", str(x)))
        out["symbol"] = spot["symbol"].astype(str)
        out["name"] = spot.get("name", pd.Series([""] * len(spot))).astype(str)
        out["open"] = spot.get("open", pd.Series([None] * len(spot))).apply(_to_float)
        out["latest"] = spot.get("latest", pd.Series([None] * len(spot))).apply(_to_float)
        out["prev_close"] = spot.get("prev_close", pd.Series([None] * len(spot))).apply(_to_float)
        out["pct_change"] = spot.get("pct_change", pd.Series([None] * len(spot))).apply(_to_float)

        mapping = {_strip_baostock_prefix(c): c for c in codes}
        out["code"] = out["symbol"].map(mapping).fillna(out["code"])
        return out.reset_index(drop=True)

    def _fetch_quotes_eastmoney(self, codes: List[str]) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()

        base_url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        fields = "f2,f3,f12,f14,f17,f18"
        secids = [_to_eastmoney_secid(c) for c in codes]

        batch_size = 80
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(secids), batch_size):
            part = secids[i : i + batch_size]
            query = {
                "fltt": "2",
                "invt": "2",
                "fields": fields,
                "secids": ",".join(part),
            }
            url = f"{base_url}?{urllib.parse.urlencode(query)}"
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Eastmoney 行情请求失败：%s", exc)
                continue

            data = (payload or {}).get("data") or {}
            diff = data.get("diff") or []
            if isinstance(diff, list):
                rows.extend([r for r in diff if isinstance(r, dict)])

        if not rows:
            return pd.DataFrame()

        out_rows: List[Dict[str, Any]] = []
        mapping = {_strip_baostock_prefix(c): c for c in codes}
        for r in rows:
            symbol = str(r.get("f12") or "").strip()
            name = str(r.get("f14") or "").strip()
            latest = _to_float(r.get("f2"))
            pct = _to_float(r.get("f3"))
            open_px = _to_float(r.get("f17"))
            prev_close = _to_float(r.get("f18"))

            code_guess = _to_baostock_code("auto", symbol)
            code = mapping.get(symbol, code_guess)

            out_rows.append(
                {
                    "code": code,
                    "symbol": symbol,
                    "name": name,
                    "open": open_px,
                    "latest": latest,
                    "prev_close": prev_close,
                    "pct_change": pct,
                }
            )

        return pd.DataFrame(out_rows)

    # -------------------------
    # Evaluate
    # -------------------------
    def _evaluate(self, signals: pd.DataFrame, quotes: pd.DataFrame, signal_date: str) -> pd.DataFrame:
        if signals.empty:
            return pd.DataFrame()

        q = quotes.copy()
        if q.empty:
            out = signals.copy()
            out["monitor_date"] = dt.date.today().isoformat()
            out["signal_date"] = signal_date
            out["open"] = None
            out["latest"] = None
            out["pct_change"] = None
            out["gap_pct"] = None
            out["action"] = "UNKNOWN"
            out["action_reason"] = "行情数据不可用"
            out["checked_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return out

        q["code"] = q["code"].astype(str)
        merged = signals.merge(q, on="code", how="left", suffixes=("", "_q"))

        merged["close"] = merged.get("close").apply(_to_float)
        merged["ma20"] = merged.get("ma20").apply(_to_float)
        merged["open"] = merged.get("open").apply(_to_float)
        merged["latest"] = merged.get("latest").apply(_to_float)
        merged["pct_change"] = merged.get("pct_change").apply(_to_float)

        def _calc_gap(row: pd.Series) -> float | None:
            close = row.get("close")
            opx = row.get("open")
            if close is None or opx is None:
                return None
            if close <= 0 or opx <= 0:
                # 开盘前/集合竞价阶段，部分接口会返回 0；当作“无今开”
                return None
            return (opx - close) / close

        merged["gap_pct"] = merged.apply(_calc_gap, axis=1)

        max_up = self.params.max_gap_up_pct
        max_down = self.params.max_gap_down_pct
        min_vs_ma20 = self.params.min_open_vs_ma20_pct
        limit_up_trigger = self.params.limit_up_trigger_pct

        actions: List[str] = []
        reasons: List[str] = []
        for _, row in merged.iterrows():
            action = "EXECUTE"
            reason = "OK"

            opx = row.get("open")
            ma20 = row.get("ma20")
            gap = row.get("gap_pct")
            pct = row.get("pct_change")

            if opx is None or opx <= 0:
                # 有些时段今开可能为空/为 0（开盘前常见），用最新价代替
                opx = row.get("latest")
                if opx is None:
                    action = "UNKNOWN"
                    reason = "无今开/最新价"
                else:
                    reason = "用最新价替代今开"

            if action == "EXECUTE" and pct is not None and pct >= limit_up_trigger:
                action = "SKIP"
                reason = f"涨幅 {pct:.2f}% 接近/达到涨停"

            if action == "EXECUTE" and gap is not None and gap > max_up:
                action = "SKIP"
                reason = f"高开 {gap*100:.2f}% 超过阈值 {max_up*100:.2f}%"

            if action == "EXECUTE" and gap is not None and gap < max_down:
                action = "SKIP"
                reason = f"低开 {gap*100:.2f}% 低于阈值 {max_down*100:.2f}%"

            if action == "EXECUTE" and (ma20 is not None) and (opx is not None):
                threshold = ma20 * (1.0 + min_vs_ma20)
                if opx < threshold:
                    action = "SKIP"
                    reason = f"开盘价 {opx:.2f} 跌破 MA20 阈值 {threshold:.2f}"

            actions.append(action)
            reasons.append(reason)

        merged["monitor_date"] = dt.date.today().isoformat()
        merged["signal_date"] = signal_date
        merged["action"] = actions
        merged["action_reason"] = reasons
        merged["checked_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        keep_cols = [
            "monitor_date",
            "signal_date",
            "date",
            "code",
            "name",
            "close",
            "open",
            "latest",
            "pct_change",
            "gap_pct",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "vol_ratio",
            "macd_hist",
            "kdj_k",
            "kdj_d",
            "atr14",
            "stop_ref",
            "signal",
            "reason",
            "action",
            "action_reason",
            "checked_at",
        ]
        for col in keep_cols:
            if col not in merged.columns:
                merged[col] = None

        return merged[keep_cols].copy()

    # -------------------------
    # Persist & export
    # -------------------------
    def _persist_results(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        table = self.params.output_table
        if not self.params.write_to_db:
            return

        monitor_date = str(df.iloc[0].get("monitor_date") or "").strip()
        codes = df["code"].dropna().astype(str).unique().tolist()

        if monitor_date and codes and self._table_exists(table):
            delete_stmt = text(
                "DELETE FROM `{table}` WHERE `monitor_date` = :d AND `code` IN :codes".format(
                    table=table
                )
            ).bindparams(bindparam("codes", expanding=True))
            try:
                with self.db_writer.engine.begin() as conn:
                    conn.execute(delete_stmt, {"d": monitor_date, "codes": codes})
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("开盘监测表去重删除失败，将直接追加：%s", exc)

        try:
            self.db_writer.write_dataframe(df, table, if_exists="append")
            self.logger.info("开盘监测结果已写入表 %s：%s 条", table, len(df))
        except Exception as exc:  # noqa: BLE001
            self.logger.error("写入开盘监测表失败：%s", exc)

    def _export_csv(self, df: pd.DataFrame) -> None:
        if df.empty or (not self.params.export_csv):
            return

        app_sec = get_section("app") or {}
        base_dir = "output"
        if isinstance(app_sec, dict):
            base_dir = str(app_sec.get("output_dir", base_dir))

        outdir = Path(base_dir) / self.params.output_subdir
        outdir.mkdir(parents=True, exist_ok=True)

        monitor_date = str(df.iloc[0].get("monitor_date") or dt.date.today().isoformat())
        path = outdir / f"open_monitor_{monitor_date}.csv"

        export_df = df.copy()
        export_df["gap_pct"] = export_df["gap_pct"].apply(_to_float)
        export_df = export_df.sort_values(by=["action", "gap_pct"], ascending=[True, True])
        if self.params.export_top_n > 0:
            export_df = export_df.head(self.params.export_top_n)

        export_df.to_csv(path, index=False, encoding="utf-8-sig")
        self.logger.info("开盘监测 CSV 已导出：%s", path)

    # -------------------------
    # Public run
    # -------------------------
    def run(self) -> None:
        if not self.params.enabled:
            self.logger.info("open_monitor.enabled=false，跳过开盘监测。")
            return

        signal_date, signals = self._load_latest_buy_signals()
        if not signal_date or signals.empty:
            return

        codes = signals["code"].dropna().astype(str).unique().tolist()
        self.logger.info("待监测标的数量：%s", len(codes))

        quotes = self._fetch_quotes(codes)
        if quotes.empty:
            self.logger.warning("未获取到任何实时行情，将输出 UNKNOWN 结果。")
        else:
            self.logger.info("实时行情已获取：%s 条", len(quotes))

        result = self._evaluate(signals, quotes, signal_date)
        if result.empty:
            return

        summary = result["action"].value_counts(dropna=False).to_dict()
        self.logger.info("开盘监测结果统计：%s", summary)

        exec_df = result[result["action"] == "EXECUTE"].copy()
        exec_df["gap_pct"] = exec_df["gap_pct"].apply(_to_float)
        exec_df = exec_df.sort_values(by="gap_pct", ascending=True)
        top_n = min(30, len(exec_df))
        if top_n > 0:
            preview = exec_df[
                ["code", "name", "close", "open", "latest", "gap_pct", "action_reason"]
            ].head(top_n)
            self.logger.info(
                "可执行清单 Top%s（按 gap 由小到大）：\n%s",
                top_n,
                preview.to_string(index=False),
            )

        self._persist_results(result)
        self._export_csv(result)
