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
        frames = []
        for _, group in df.groupby("code", sort=False):
            frames.append(wyck_model.run(group))
        df = pd.concat(frames, ignore_index=True) if frames else df.copy()
        
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
        cols = [
            "close",
            "ma5",
            "ma20",
            "ma60",
            "ma250",
            "ma20_bias",
            "vol_ratio",
            "atr14",
            "volume",
            "amount",
            "rsi14",
        ]
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

        min_daily_amount = float(self.params.get("min_daily_amount", 0.0) or 0.0)
        if min_daily_amount > 0:
            amount = pd.to_numeric(df.get("amount"), errors="coerce")
            low_liquidity = amount.isna() | (amount < min_daily_amount)
        else:
            low_liquidity = pd.Series(False, index=df.index)

        # --- 新增：股价上限过滤 ---
        max_price = float(self.params.get("max_stock_price", 999999.0) or 999999.0)
        # 移除硬性拦截，改为仅打标签，以便观察高价龙头
        # too_expensive = df["close"] > max_price
            
        return missing | one_word | env_gate | low_liquidity

    def _calc_base_signals(self, df: pd.DataFrame) -> dict:
        """计算基础买卖信号。"""
        p = self.params
        c, ma5, ma20, ma60 = df["close"], df["ma5"], df["ma20"], df["ma60"]
        prev_ma5, prev_ma20 = df["prev_ma5"], df["prev_ma20"]

        trend_ok = (c > df["ma250"]) & (ma20 > df["ma250"])
        
        # 1. 准备量能参考：5日均量
        df["vol_ma5"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(5).mean())
        
        # 2. 金叉买入
        cross_up = (ma5 > ma20) & (prev_ma5 <= prev_ma20)
        buy_cross = trend_ok & cross_up
        
        # 3. 回踩买入重构 (引入 VSA)：必须是缩量回踩或平量回踩，不能是放量大跌
        pullback_atr_mult = float(p.get("pullback_atr_mult", 0.6))
        atr_ok = df["atr14"].notna() & (df["atr14"] > 0)
        dist_to_ma20 = (c - ma20).abs()
        # VSA 条件：当前量 < 5日均量的 1.05 倍 (防止放量砸盘)
        vol_vsa_ok = df["volume"] < df["vol_ma5"] * 1.05
        
        buy_pullback = (
            trend_ok
            & atr_ok
            & (dist_to_ma20 <= pullback_atr_mult * df["atr14"])
            & (ma5 > prev_ma5)
            & vol_vsa_ok
        )
        
        base_buy = buy_cross | buy_pullback
        
        # 4. 止损逻辑重构 (增加 ATR 缓冲区)：防止假破位洗盘
        # 原逻辑：c < ma20
        # 新逻辑：c < ma20 - 0.3 * ATR
        stop_buffer_atr = float(p.get("stop_buffer_atr", 0.3))
        min_stop_pct = 0.005
        min_stop_price = ma20 * min_stop_pct
        hard_stop = atr_ok & (c < (ma20 - (stop_buffer_atr * df["atr14"] + min_stop_price)))

        # --- 新增：Trailing Stop (移动止盈) ---
        # 逻辑：价格从近 20 日最高点回撤超过 3.0 * ATR，强制止盈
        # 用于保护主升浪利润，防止过山车
        rolling_high_20 = df.groupby("code")["close"].transform(lambda x: x.rolling(20).max())
        trailing_atr_mult = 3.0
        trailing_stop = atr_ok & (c < (rolling_high_20 - trailing_atr_mult * df["atr14"]))

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
        
        base_sell = dead_cross | stagnation | hard_stop | trailing_stop
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
        out["risk_tag"] = ""
        
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
            out["rs_daily"] = np.nan
            out["rs_5d"] = np.nan

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
        rs_5d = out["rs_5d"]
        rs_factor = np.where(
            rs_5d.notna(),
            np.where(rs_5d > 0.05, 1.5, np.where(rs_5d > 0, 0.5, -0.5)),
            0.0,
        )
        # B. 逆市表现加分 (大盘跌 > 0.5%, 个股红盘)
        out["is_resilient"] = (out.get("index_ret", np.nan) < -0.005) & (out["pct_chg"] > 0)
        resilient_bonus = np.where(out["is_resilient"], 1.0, 0.0)
        
        # --- 新增：RSI 动能因子 ---
        rsi = out.get("rsi14", pd.Series(50, index=out.index))
        # 动能适中区 (40-65) 加分，过热区 (>75) 强力扣分
        rsi_factor = np.select(
            [rsi > 75, (rsi >= 40) & (rsi <= 65)],
            [-2.0, 0.5], default=0.0
        )

        # --- 新增：RPS 相对强度因子 (优化版：兼顾龙头与黑马) ---
        # 1. 获取数据
        rps_50 = out.get("rps_50", pd.Series(0, index=out.index)).fillna(0)
        rps_120 = out.get("rps_120", pd.Series(0, index=out.index)).fillna(0)
        ma250 = out.get("ma250", pd.Series(1e-6, index=out.index)) # 避免除零
        
        # 2. 判定是否为“低位” (安全边际区)
        # 股价距离年线不到 15%，且在年线上方 (Trend OK 已保证上方)
        bias_ma250 = (out["close"] - ma250) / ma250
        is_low_base = (bias_ma250 < 0.15) & (bias_ma250 > 0)
        
        # 3. 判定是否“进步飞快” (黑马特征)
        # 短期排名显著高于中期排名
        is_improving = rps_50 > (rps_120 + 10)
        
        # 4. 综合打分
        # 逻辑 A: 顶级龙头 (RPS > 90) -> 必买 (+2.0)
        # 逻辑 B: 低位黑马 (低位 + 进步快) -> 鼓励 (+1.5)
        # 逻辑 C: 低位潜伏 (低位 + RPS一般) -> 宽容 (0.0，不扣分)
        # 逻辑 D: 高位滞涨 (高位 + RPS低) -> 严惩 (-2.0，甚至直接拦截)
        
        rps_factor = np.select(
            [
                rps_50 >= 90, 
                is_low_base & is_improving,
                is_low_base,
                (rps_50 < 70) & (~is_low_base)
            ],
            [2.0, 1.5, 0.5, -2.0], 
            default=0.0
        )

        # --- 新增：动能加速因子 (Momentum Acceleration) ---
        # 逻辑：短期速率 > 中期速率 > 长期速率，且必须是正收益
        # 奖励那些“越涨越快”的主升浪标的
        ret_20 = out.get("ret_20", pd.Series(0, index=out.index)).fillna(0)
        ret_50 = out.get("ret_50", pd.Series(0, index=out.index)).fillna(0)
        ret_120 = out.get("ret_120", pd.Series(0, index=out.index)).fillna(0)
        
        # 归一化为日均涨幅近似值 (简单除法即可，因为只比大小)
        v_short = ret_20 / 20.0
        v_mid = ret_50 / 50.0
        v_long = ret_120 / 120.0
        
        is_accelerating = (v_short > v_mid) & (v_mid > v_long) & (v_long > 0)
        # 只有在 RPS 也是强者(>80)的情况下，加速才有意义 (避免垃圾股的超跌反弹加速)
        acceleration_bonus = np.where(is_accelerating & (rps_50 > 80), 1.0, 0.0)

        # --- 新增：防追涨惩罚 (Anti-Chase Penalty) ---
        # 逻辑：即使是好票，如果离 MA20 太远 (偏离度 > 10%)，也视为追涨
        ma20_bias = out.get("ma20_bias", pd.Series(0, index=out.index)).fillna(0)
        chase_penalty = np.where(ma20_bias > 0.10, -3.0, 0.0)

        quality = (board_factor * 2.0) + chip_factor + (wyckoff_s * w_wyckoff) + (engulf_s * w_engulf) + rs_factor + resilient_bonus + dist_factor + acc_factor + trend_up_factor + rsi_factor + rps_factor + acceleration_bonus + chase_penalty
        out["quality_score"] = quality

        def _add_tag(mask: pd.Series, tag: str) -> None:
            if not mask.any():
                return
            existing = out.loc[mask, "risk_tag"].fillna("").astype(str)
            out.loc[mask, "risk_tag"] = np.where(
                existing == "",
                tag,
                existing + "," + tag,
            )

        atr_missing = out.get("atr14").isna() if "atr14" in out.columns else pd.Series(True, index=out.index)
        _add_tag(atr_missing, "DATA_MISSING_ATR")

        if "index_ret" in out.columns:
            index_missing = out["index_ret"].isna()
        else:
            index_missing = pd.Series(True, index=out.index)
        _add_tag(index_missing, "DATA_MISSING_INDEX")

        min_daily_amount = float(self.params.get("min_daily_amount", 0.0) or 0.0)
        if min_daily_amount > 0:
            amount = pd.to_numeric(out.get("amount"), errors="coerce")
            _add_tag(amount.isna(), "DATA_MISSING_AMOUNT")
            _add_tag((amount < min_daily_amount) & amount.notna(), "LOW_LIQUIDITY")

        # --- 新增：高价股标签 ---
        max_price = float(self.params.get("max_stock_price", 999999.0) or 999999.0)
        _add_tag(out["close"] > max_price, "PRICE_TOO_HIGH")
        
        # 3. 动态调整 (根据质量分拦截或升级)
        # --- 新增：Wyckoff 强制硬拦截 ---
        # 如果是派发阶段 且 出现了 SOW(供应出现) 事件，强制 SELL
        wyckoff_event = out.get("wyckoff_event", pd.Series("", index=out.index))
        is_sow = (wyckoff_phase == "DISTRIBUTION") & (wyckoff_event == "SOW")
        out.loc[is_sow, "signal"] = "SELL"
        out.loc[is_sow, "reason"] = "Wyckoff派发确认: SOW破位"

        is_buy = out["signal"] == "BUY"
        
        # --- 环境感知动态门槛 (Scenario C 优化) ---
        # 默认阈值
        default_thresh = float(self.params.get("quality_stop_threshold", -3.0))
        
        # 计算指数 5 日累计收益 (环境动量)
        if "index_ret" in out.columns:
            # 填充 NaN 为 0 以免计算出错
            idx_ret_filled = out["index_ret"].fillna(0.0)
            # 注意：这里的 rolling 是针对 index_ret 列，但该列对同一天所有股票是相同的
            # 所以直接取即可，不需要 groupby code (虽然 groupby 也没错且更安全)
            index_ret_5d = out.groupby("code")["index_ret"].transform(lambda x: x.rolling(5).sum())
        else:
            index_ret_5d = pd.Series(0.0, index=out.index)
            
        # 如果大盘 5 日累计跌幅超过 2% (弱势/阴跌)，提高门槛到 0.0
        # 要求个股必须有足够的加分项 (RS强、板块热、筹码好) 才能开仓
        dynamic_thresh = np.where(index_ret_5d < -0.02, 0.0, default_thresh)
        
        # 质量太差 -> 拦截
        mask_stop = is_buy & (quality <= dynamic_thresh)
        out.loc[mask_stop, "signal"] = "WAIT"
        out.loc[mask_stop, "reason"] += "|质量分过低(环境自适应)"
        
        # 4. 计算仓位
        base_cap = 0.5
        # 质量分越高仓位越高，线性映射 [0.0, 0.8]
        # 简单算法：base + quality * 0.1
        final_cap = base_cap + (quality * 0.1)
        final_cap = final_cap.clip(0.0, 0.8)
        
        # 非买入信号仓位为0 (HOLD/SELL)
        out["final_cap"] = np.where(out["signal"].isin(["BUY", "BUY_CONFIRM"]), final_cap, 0.0)
        
        return out
