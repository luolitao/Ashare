"""低吸反转策略。

核心逻辑：
1. 趋势背景：长期趋势向上 (MA60/MA250 向上)。
2. 短期超跌：偏离 MA20 过远 (BIAS < -5%) 或 RSI 超卖。
3. 支撑确认：回踩重要均线 (MA60/MA250) 或 布林下轨。
4. 企稳信号：缩量、长下影、阳包阴。
"""

import pandas as pd
import numpy as np
from ashare.strategies.base import BaseStrategy
from ashare.strategies.factory import register_strategy


@register_strategy("low_suck_reversal")
class LowSuckStrategy(BaseStrategy):
    """日线级别低吸反转策略。"""

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        p = self.params
        if df.empty:
            return df

        frames = []
        for _, group in df.groupby("code", sort=False):
            out = group.copy().sort_values("date")

            # 1. 准备数据
            for col in [
                "close", "low", "high", "open", "volume",
                "ma20", "ma60", "ma250", "atr14", "vol_ratio", "ma20_bias",
            ]:
                if col not in out.columns:
                    out[col] = np.nan

            close = out["close"]
            low = out["low"]
            high = out["high"]
            open_ = out["open"]
            volume = out["volume"]
            ma20 = out["ma20"]
            ma60 = out["ma60"]
            ma250 = out["ma250"]

            bias20 = (close - ma20) / ma20
            rsi = out.get("rsi14", pd.Series(50, index=out.index))

            # 2. 趋势与风险过滤
            trend_ok = (ma60 > ma60.shift(5)) | (ma250 > ma250.shift(10))
            trend_bad = (ma60 < ma60.shift(10)) & (ma20 < ma60) & (close < ma20)

            # 飞刀判定重构：由 -12% 改为 3.0 * ATR
            # fall_thresh = float(p.get("falling_knife_ret_10", -0.12))
            atr14 = out["atr14"]
            fall_atr_mult = float(p.get("falling_knife_atr_mult", 3.0))
            falling_knife = ((close.shift(10) - close) > fall_atr_mult * atr14) & trend_bad

            # 3. 超跌条件重构：由 -6% 改为 1.5 * ATR
            # bias_thresh = float(p.get("bias20_threshold", -0.06))
            bias_atr_mult = float(p.get("bias20_atr_mult", 1.5))
            oversold_bias = (ma20 - close) >= bias_atr_mult * atr14

            rsi_thresh = float(p.get("rsi_threshold", 30))
            oversold_rsi = rsi <= rsi_thresh

            is_oversold = oversold_bias | oversold_rsi

            # 4. 支撑验证重构：由 1% 精度改为 0.2 * ATR
            support_atr_tol = float(p.get("support_atr_tol", 0.2))
            near_ma60 = (low <= ma60 + support_atr_tol * atr14) & (close >= ma60 - support_atr_tol * atr14)
            near_ma250 = (low <= ma250 + support_atr_tol * atr14) & (close >= ma250 - support_atr_tol * atr14)
            recent_low = low.rolling(20, min_periods=10).min()
            near_recent_low = low <= recent_low + support_atr_tol * atr14
            has_support = near_ma60 | near_ma250 | near_recent_low

            # 5. 反转形态与量能确认
            body = (close - open_).abs()
            lower_shadow = np.minimum(close, open_) - low
            # 下影线重构：由 1% 价格改为 0.25 * ATR
            hammer = (lower_shadow > body * 2.0) & (lower_shadow > 0.25 * atr14)

            prev_close = close.shift(1)
            prev_open = open_.shift(1)
            prev_high = high.shift(1)
            engulfing = (
                (close > open_) & (prev_close < prev_open) &
                (close > prev_open) & (open_ < prev_close)
            )
            reclaim_high = close > prev_high

            avg_volume_20 = out.get("avg_volume_20")
            if isinstance(avg_volume_20, pd.Series):
                vol_confirm = volume >= avg_volume_20 * float(p.get("rebound_vol_ratio", 1.1))
            else:
                vol_confirm = volume > volume.shift(1)

            reversal_pattern = (hammer | engulfing | reclaim_high) & vol_confirm

            # 6. 波动与量能收缩
            atr_pct = (out.get("atr14") / close).replace([np.inf, -np.inf], np.nan)
            atr_max = float(p.get("atr_pct_max", 0.035))
            vol_contract = atr_pct <= atr_max

            vol_ratio = out.get("vol_ratio", pd.Series(np.nan, index=out.index))
            vol_ratio_max = float(p.get("vol_ratio_max", 1.2))
            vol_ratio_ok = vol_ratio <= vol_ratio_max

            # 7. 生成信号
            mode = str(p.get("mode", "conservative")).lower()
            base_ok = trend_ok & (~trend_bad) & (~falling_knife)

            if mode == "aggressive":
                buy_sig = base_ok & is_oversold & has_support & vol_contract
            else:
                buy_sig = base_ok & is_oversold & has_support & reversal_pattern & vol_contract & vol_ratio_ok

            out["signal"] = "HOLD"
            out.loc[buy_sig, "signal"] = "BUY"

            # 8. 填写原因
            reasons = pd.Series("", index=out.index)
            reasons = reasons.mask(oversold_bias, "乖离超跌")
            rsi_mask = oversold_rsi & (reasons != "")
            reasons = reasons.mask(rsi_mask, reasons + "|RSI超卖")
            reasons = reasons.mask(oversold_rsi & (reasons == ""), "RSI超卖")
            reasons = reasons.mask(near_ma60 & (reasons != ""), reasons + "|回踩MA60")
            reasons = reasons.mask(near_ma250 & (reasons != ""), reasons + "|回踩MA250")
            reasons = reasons.mask(near_recent_low & (reasons != ""), reasons + "|接近近期低位")
            reasons = reasons.mask(reversal_pattern & (reasons != ""), reasons + "|反转确认")
            reasons = reasons.mask(vol_contract & (reasons != ""), reasons + "|波动收缩")

            out.loc[buy_sig, "reason"] = "低吸: " + reasons
            out["risk_tag"] = None
            out.loc[falling_knife, "risk_tag"] = "LOW_SUCK_KNIFE"

            frames.append(out)

        return pd.concat(frames, ignore_index=True) if frames else df.copy()
