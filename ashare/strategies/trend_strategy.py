"""趋势跟随策略 (MA5 + MA20)。

重构版本：继承自 BaseStrategy，只负责核心计算。
"""

import json
import logging
import numpy as np
import pandas as pd
from sqlalchemy import text, bindparam

from ashare.core.db import MySQLWriter, DatabaseConfig
from ashare.core.schema_manager import TABLE_STRATEGY_CHIP_FILTER
from ashare.strategies.base import BaseStrategy
from ashare.strategies.factory import register_strategy


@register_strategy("ma5_ma20_trend")
class TrendStrategy(BaseStrategy):
    """
    MA5/MA20 趋势跟随策略。
    包含：
    1. HardGate: 涨停、停牌、环境门控
    2. BaseSignal: 均线金叉、多头排列
    3. Factors: 量比、MACD、筹码、威科夫、吞没形态
    """

    def __init__(self, params):
        super().__init__(params)
        self.logger = logging.getLogger(self.__class__.__name__)
        # 为了兼容读取筹码数据，暂时保留 DB 连接
        # 理想情况下，所有数据应由 Runner 准备好传入
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """执行策略计算逻辑。"""
        # 1. 预处理
        df = self._prepare_data(df)
        
        # 2. 计算威科夫底层状态 (全局状态机)
        from ashare.strategies.ma_wyckoff_model import MAWyckoffStrategy
        wyck_model = MAWyckoffStrategy()
        df = wyck_model.run(df) # 注入 wyckoff_phase, wyckoff_event 等
        
        # 3. 计算各层级信号
        hard_gate = self._calc_hard_gate(df)
        base_signals = self._calc_base_signals(df)
        soft_factors = self._calc_soft_factors(df)
        
        # 4. 关联筹码数据 (IO 操作)
        chip_df = self._fetch_chip_data(df)
        if not chip_df.empty:
            df = df.merge(chip_df, on=["date", "code"], how="left")
        
        # 5. 综合决策
        result = self._combine_signals(df, hard_gate, base_signals, soft_factors)
        
        return result

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["code", "date"]).copy()
        # 确保列存在
        cols = ["close", "ma5", "ma20", "ma60", "ma20_bias", "vol_ratio", "atr14", "volume", "rsi14"]
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        
        # 计算前值
        df["prev_ma5"] = df.groupby("code")["ma5"].shift(1)
        df["prev_ma20"] = df.groupby("code")["ma20"].shift(1)
        df["prev_vol"] = df.groupby("code")["volume"].shift(1)
        df["pct_chg"] = df.groupby("code")["close"].pct_change()
        
        return df

    def _calc_hard_gate(self, df: pd.DataFrame) -> pd.Series:
        """计算硬门槛 (Hard Gate)。"""
        # 缺数据
        missing = df[["close", "ma5", "ma20"]].isna().any(axis=1)
        
        # 一字涨停 (假设列已存在，若不存在则为 False)
        one_word = self._as_bool_series(df.get("one_word_limit_up"), df.index)
        
        # 环境门控 (检查是否有 external_gate_action 列)
        env_gate = pd.Series(False, index=df.index)
        if "env_gate_action" in df.columns:
            env_gate = df["env_gate_action"].fillna("").astype(str).str.upper().isin(["STOP", "ALLOW_NONE"])
            
        return missing | one_word | env_gate

    def _calc_base_signals(self, df: pd.DataFrame) -> dict:
        """计算基础买卖信号。"""
        p = self.params
        c, ma5, ma20, ma60 = df["close"], df["ma5"], df["ma20"], df["ma60"]
        prev_ma5, prev_ma20 = df["prev_ma5"], df["prev_ma20"]

        trend_ok = (c > ma60) & (ma20 > ma60)
        
        # 1. 准备量能参考：5日均量
        df["vol_ma5"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(5).mean())
        
        # 2. 金叉买入
        cross_up = (ma5 > ma20) & (prev_ma5 <= prev_ma20)
        buy_cross = trend_ok & cross_up
        
        # 3. 回踩买入重构 (引入 VSA)：必须是缩量回踩或平量回踩，不能是放量大跌
        pullback_atr_mult = float(p.get("pullback_atr_mult", 0.6))
        dist_to_ma20 = (c - ma20).abs()
        # VSA 条件：当前量 < 5日均量的 1.1 倍 (防止放量砸盘)
        vol_vsa_ok = df["volume"] < df["vol_ma5"] * 1.1
        
        buy_pullback = trend_ok & (dist_to_ma20 <= pullback_atr_mult * df["atr14"]) & (ma5 > prev_ma5) & vol_vsa_ok
        
        base_buy = buy_cross | buy_pullback
        
        # 4. 止损逻辑重构 (增加 ATR 缓冲区)：防止假破位洗盘
        # 原逻辑：c < ma20
        # 新逻辑：c < ma20 - 0.3 * ATR
        stop_buffer_atr = float(p.get("stop_buffer_atr", 0.3))
        hard_stop = c < (ma20 - stop_buffer_atr * df["atr14"])
        
        dead_cross = (ma5 < ma20) & (prev_ma5 >= prev_ma20)
        
        # 滞涨判定
        stag_vol = float(p.get("stagnation_vol_ratio_threshold", 2.0))
        stag_atr_mult = float(p.get("stagnation_atr_mult", 0.2)) 
        stag_bias_atr_mult = float(p.get("stagnation_bias_atr_mult", 2.5)) 
        
        stagnation = (
            (df["vol_ratio"] >= stag_vol) & 
            (df["pct_chg"].abs() * c < stag_atr_mult * df["atr14"]) & 
            (dist_to_ma20 > stag_bias_atr_mult * df["atr14"])
        )
        
        base_sell = dead_cross | stagnation | hard_stop
        base_reduce = (ma5 < ma20) & (c < ma20) & (~dead_cross) & (~hard_stop)
        
        return {
            "buy": base_buy,
            "sell": base_sell,
            "reduce": base_reduce,
            "cross_up": cross_up,
            "trend_ok": trend_ok
        }

    def _calc_soft_factors(self, df: pd.DataFrame) -> dict:
        """计算软因子 (威科夫、吞没等)。"""
        # 这里为了简化，假设 wyckoff_score 和 engulf_score 已经在指标计算阶段算好并合入了 df
        # 如果没有，则给默认值
        wyckoff_score = df.get("wyckoff_score", pd.Series(0.0, index=df.index)).fillna(0.0)
        engulf_score = df.get("engulf_score", pd.Series(0.0, index=df.index)).fillna(0.0)
        
        return {
            "wyckoff": wyckoff_score,
            "engulf": engulf_score
        }

    def _fetch_chip_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """(临时) 主动去查筹码表。"""
        if df.empty:
            return pd.DataFrame()
        
        codes = df["code"].unique().tolist()
        latest_date = df["date"].dt.date.max()
        
        try:
            stmt = text(
                f"SELECT sig_date as date, code, chip_score "
                f"FROM {TABLE_STRATEGY_CHIP_FILTER} "
                "WHERE sig_date = :d AND code IN :codes"
            )
            with self.db_writer.engine.connect() as conn:
                chip_df = pd.read_sql(
                    stmt.bindparams(bindparam("codes", expanding=True)), 
                    conn, 
                    params={"d": latest_date, "codes": codes}
                )
            if not chip_df.empty:
                chip_df["date"] = pd.to_datetime(chip_df["date"])
                return chip_df
        except Exception as e:
            self.logger.warning("筹码数据查询失败: %s", e)
        
        return pd.DataFrame()

    def _combine_signals(self, df: pd.DataFrame, hard_gate, base, soft) -> pd.DataFrame:
        """综合打分与决策。"""
        out = df.copy()
        
        # 1. 初始信号
        signal = np.select(
            [hard_gate, base["sell"], base["reduce"], base["buy"]],
            ["HOLD", "SELL", "REDUCE", "BUY"],
            default="HOLD"
        )
        
        out["signal"] = signal
        out["reason"] = "观望"
        out.loc[base["buy"] & base["cross_up"], "reason"] = "趋势金叉"
        
        # 2. 质量分 (Quality Score)
        # 包含：威科夫、吞没、板块(rotation_phase)、筹码(chip_score)、RS(相对强度)
        
        # 计算 RS (相对强度)
        # 简单定义：个股涨跌幅 - 指数涨跌幅
        if "index_ret" in out.columns:
            out["rs_daily"] = out["pct_chg"] - out["index_ret"]
            # 计算 5 日滚动 RS 以识别持续走强品种
            out["rs_5d"] = out.groupby("code")["rs_daily"].transform(lambda x: x.rolling(5).sum())
        else:
            out["rs_daily"] = 0.0
            out["rs_5d"] = 0.0

        # 归一化各因子
        wyckoff_phase = out.get("wyckoff_phase", pd.Series("NONE", index=out.index))
        # 派发阶段因子：强力扣分
        dist_factor = np.where(wyckoff_phase == "DISTRIBUTION", -3.0, 0.0)
        # 吸筹阶段因子：中力加分 (等待突破)
        acc_factor = np.where(wyckoff_phase == "ACCUMULATION", 1.0, 0.0)
        # 趋势向上阶段：小力加分
        trend_up_factor = np.where(wyckoff_phase == "TREND_UP", 0.5, 0.0)

        if "wyckoff" in soft and hasattr(soft["wyckoff"], "fillna"):
             wyckoff_s = pd.to_numeric(soft["wyckoff"], errors="coerce").fillna(0)
        else:
             wyckoff_s = pd.Series(0, index=out.index)

        if "engulf" in soft and hasattr(soft["engulf"], "fillna"):
             engulf_s = pd.to_numeric(soft["engulf"], errors="coerce").fillna(0)
        else:
             engulf_s = pd.Series(0, index=out.index)

        # 安全获取 chip_score
        raw_chip = out.get("chip_score")
        if isinstance(raw_chip, pd.Series):
            chip_s = pd.to_numeric(raw_chip, errors="coerce").fillna(0)
        else:
            # 如果不存在或为标量，构造全0 Series
            chip_s = pd.Series(0, index=out.index)

        chip_factor = np.where(chip_s >= 0.5, 1.0, np.where(chip_s <= -0.5, -1.0, 0.0))
        
        # 板块因子
        if "rotation_phase" in out.columns:
            board_phase = out["rotation_phase"].fillna("neutral")
        else:
            board_phase = pd.Series("neutral", index=out.index)

        board_factor = np.select(
            [board_phase.isin(["leader", "leading"]), board_phase == "improving"],
            [2.0, 1.0], default=0.0
        )
        
        # 加权求和
        w_wyckoff = float(self.params.get("wyckoff_score_weight", 0.5))
        w_engulf = float(self.params.get("engulf_score_weight", 0.3))
        
        # RS 核心逻辑加分：
        # A. 5日 RS 持续走强
        rs_factor = np.where(out["rs_5d"] > 0.05, 1.5, np.where(out["rs_5d"] > 0, 0.5, -0.5))
        # B. 逆市表现加分 (大盘跌 > 0.5%, 个股红盘)
        out["is_resilient"] = (out.get("index_ret", 0) < -0.005) & (out["pct_chg"] > 0)
        resilient_bonus = np.where(out["is_resilient"], 1.0, 0.0)
        
        # --- 新增：RSI 动能因子 ---
        rsi = out.get("rsi14", pd.Series(50, index=out.index))
        # 动能适中区 (40-65) 加分，过热区 (>75) 强力扣分
        rsi_factor = np.select(
            [rsi > 75, (rsi >= 40) & (rsi <= 65)],
            [-2.0, 0.5], default=0.0
        )

        quality = (board_factor * 1.5) + chip_factor + (wyckoff_s * w_wyckoff) + (engulf_s * w_engulf) + rs_factor + resilient_bonus + dist_factor + acc_factor + trend_up_factor + rsi_factor
        out["quality_score"] = quality
        
        # 3. 动态调整 (根据质量分拦截或升级)
        # --- 新增：Wyckoff 强制硬拦截 ---
        # 如果是派发阶段 且 出现了 SOW(供应出现) 事件，强制 SELL
        wyckoff_event = out.get("wyckoff_event", pd.Series("", index=out.index))
        is_sow = (wyckoff_phase == "DISTRIBUTION") & (wyckoff_event == "SOW")
        out.loc[is_sow, "signal"] = "SELL"
        out.loc[is_sow, "reason"] = "Wyckoff派发确认: SOW破位"

        is_buy = out["signal"] == "BUY"
        stop_thresh = float(self.params.get("quality_stop_threshold", -3.0))
        
        # 质量太差 -> 拦截
        mask_stop = is_buy & (quality <= stop_thresh)
        out.loc[mask_stop, "signal"] = "WAIT"
        out.loc[mask_stop, "reason"] += "|质量分过低"
        
        # 4. 计算仓位
        base_cap = 0.5
        # 质量分越高仓位越高，线性映射 [0.0, 0.8]
        # 简单算法：base + quality * 0.1
        final_cap = base_cap + (quality * 0.1)
        final_cap = final_cap.clip(0.0, 0.8)
        
        # 非买入信号仓位为0 (HOLD/SELL)
        out["final_cap"] = np.where(out["signal"].isin(["BUY", "BUY_CONFIRM"]), final_cap, 0.0)
        
        return out
