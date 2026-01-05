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
        out = df.copy()
        
        # 1. 准备数据
        # 确保基本列存在
        for col in ["close", "low", "high", "open", "ma20", "ma60", "ma250"]:
            if col not in out.columns:
                out[col] = np.nan
        
        close = out["close"]
        low = out["low"]
        high = out["high"]
        open_ = out["open"]
        ma20 = out["ma20"]
        ma60 = out["ma60"]
        ma250 = out["ma250"]
        
        bias20 = (close - ma20) / ma20
        rsi = out.get("rsi14", pd.Series(50, index=out.index))
        
        # 2. 长期趋势筛选
        trend_ok = (ma60 > ma60.shift(5)) | (ma250 > ma250.shift(10))
        
        # 3. 超跌条件
        bias_thresh = float(p.get("bias20_threshold", -0.06))
        oversold_bias = bias20 <= bias_thresh
        
        rsi_thresh = float(p.get("rsi_threshold", 30))
        oversold_rsi = rsi <= rsi_thresh
        
        is_oversold = oversold_bias | oversold_rsi
        
        # 4. 支撑验证
        near_ma60 = (low <= ma60 * 1.01) & (close >= ma60 * 0.99)
        near_ma250 = (low <= ma250 * 1.01) & (close >= ma250 * 0.99)
        has_support = near_ma60 | near_ma250
        
        # 5. 反转形态
        body = (close - open_).abs()
        lower_shadow = np.minimum(close, open_) - low
        hammer = (lower_shadow > body * 2.0) & (lower_shadow > close * 0.01)
        
        prev_close = close.shift(1)
        prev_open = open_.shift(1)
        engulfing = (
            (close > open_) & (prev_close < prev_open) & 
            (close > prev_open) & (open_ < prev_close)
        )
        
        reversal_pattern = hammer | engulfing
        
        # 6. 生成信号
        mode = str(p.get("mode", "conservative")).lower()
        
        if mode == "aggressive":
            buy_sig = trend_ok & is_oversold & has_support
        else:
            buy_sig = trend_ok & is_oversold & has_support & reversal_pattern
            
        out["signal"] = "HOLD"
        out.loc[buy_sig, "signal"] = "BUY"
        
        # 7. 填写原因
        reasons = pd.Series("", index=out.index)
        reasons = reasons.mask(oversold_bias, "乖离超跌")
        # 累加原因文本需要确保类型一致
        rsi_mask = oversold_rsi & (reasons != "")
        reasons = reasons.mask(rsi_mask, reasons + "|RSI超卖")
        reasons = reasons.mask(oversold_rsi & (reasons == ""), "RSI超卖")
        
        sup_mask = near_ma60 & (reasons != "")
        reasons = reasons.mask(sup_mask, reasons + "|回踩MA60")
        
        out.loc[buy_sig, "reason"] = "低吸: " + reasons
        out["risk_tag"] = None
        
        return out
