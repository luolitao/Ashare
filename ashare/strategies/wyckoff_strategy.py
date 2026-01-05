"""威科夫派发预警策略。

基于 MA_Wyckoff_Model，识别 SOW (供应出现)、UTAD (上破回落) 等派发信号。
主要用于风控（生成 SELL/RISK 信号），而非买入。
"""

import pandas as pd
import numpy as np
from ashare.strategies.base import BaseStrategy
from ashare.strategies.factory import register_strategy
from ashare.strategies.ma_wyckoff_model import MAWyckoffStrategy, ACTION_REDUCE, ACTION_SELL, ACTION_BUY_STRONG


@register_strategy("wyckoff_distribution")
class WyckoffStrategy(BaseStrategy):
    """威科夫派发/吸筹策略。"""

    def __init__(self, params):
        super().__init__(params)
        # 初始化核心模型
        self.model = MAWyckoffStrategy(
            ma_short=int(params.get("ma_short", 5)),
            ma_long=int(params.get("ma_long", 20)),
            efi_window=int(params.get("efi_window", 60)),
            divergence_lookback=int(params.get("divergence_lookback", 30)),
            confirmation_window=int(params.get("confirmation_window", 10)),
            long_ma_window=int(params.get("long_ma_window", 200)),
            long_slope_window=int(params.get("long_slope_window", 20)),
            structure_window=int(params.get("structure_window", 160)),
            box_len_min=int(params.get("box_len_min", 80)),
            box_volatility_cap=float(params.get("box_volatility_cap", 0.25)),
            vol_confirm_window=int(params.get("vol_confirm_window", 60)),
            vol_contract_ratio=float(params.get("vol_contract_ratio", 0.85)),
            vol_imbalance_threshold=float(params.get("vol_imbalance_threshold", 1.2)),
            breakout_pct=float(params.get("breakout_pct", 0.01)),
            reclaim_tol=float(params.get("reclaim_tol", 0.003)),
            vol_spike_mult=float(params.get("vol_spike_mult", 1.3)),
        )

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成威科夫信号。"""
        if df.empty:
            return df

        frames = []
        for _, group in df.groupby("code", sort=False):
            res = self.model.run(group)
            frames.append(res)
        res = pd.concat(frames, ignore_index=True) if frames else df.copy()
        
        # 2. 映射到标准 signal
        out = res.copy()
        out["signal"] = "HOLD"
        
        is_sell = out["action"].isin([ACTION_SELL, ACTION_REDUCE])
        is_buy = out["action"] == ACTION_BUY_STRONG
        
        out.loc[is_sell, "signal"] = "SELL"
        out.loc[is_buy, "signal"] = "BUY"
        
        # 3. 补充原因
        out["reason"] = "观望"
        
        mask_div = out["bearish_divergence"]
        mask_efi_weak = out["efi_weakness"]
        mask_death = out["death_cross"]
        mask_sow = out.get("event_sow", False)
        mask_upthrust = out.get("event_upthrust", False)
        mask_spring = out.get("event_spring", False)
        mask_sos = out.get("event_sos", False)
        
        reasons = pd.Series("", index=out.index)
        reasons = reasons.mask(mask_sow, "SOW下破放量")
        reasons = reasons.mask(mask_upthrust, "上破回落")
        reasons = reasons.mask(mask_spring, "SPRING回收")
        reasons = reasons.mask(mask_sos, "SOS突破")
        reasons = reasons.mask(mask_div, "顶部量价背离")
        reasons = reasons.mask(mask_efi_weak, "EFI动能衰竭")
        reasons = reasons.mask(mask_death, "趋势死叉确认")
        
        out.loc[is_sell, "reason"] = "派发预警: " + reasons
        out.loc[is_buy, "reason"] = "吸筹确认: 底部量价验证"
        
        # 4. 风险标签
        out["risk_tag"] = None
        out.loc[mask_div | mask_sow | mask_upthrust, "risk_tag"] = "WYCKOFF_SOW"
        
        return out
