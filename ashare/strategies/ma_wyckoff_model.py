import pandas as pd
import numpy as np
from ashare.indicators.wyckoff import WyckoffAnalyzer
from ashare.indicators.indicator_utils import macd

# 定义动作常量
ACTION_BUY_STRONG = "BUY_STRONG" # 重仓 (均线 + 量价背离确认)
ACTION_BUY_LIGHT = "BUY_LIGHT"   # 轻仓 (仅均线)
ACTION_REDUCE = "REDUCE"         # 减仓 (量价顶背离/动能衰竭)
ACTION_SELL = "SELL"             # 清仓 (均线死叉/破位)
ACTION_HOLD = "HOLD"             # 持有

WYCKOFF_SCORE_MAP = {
    ACTION_BUY_STRONG: 2.0,
    ACTION_BUY_LIGHT: 1.0,
    ACTION_REDUCE: -1.0,
    ACTION_SELL: -2.0,
    ACTION_HOLD: 0.0,
}

def calculate_ma(series: pd.Series, window: int) -> pd.Series:
    """简单移动平均 (SMA)"""
    return series.rolling(window=window).mean()

class MAWyckoffStrategy:
    """
    MA + Wyckoff 融合策略 (4档动作模型)
    
    整合逻辑：
    1. MA5/MA20 提供趋势基础 (Trend)
    2. Wyckoff EFI & MACD Divergence 提供动能健康度诊断 (Momentum Health)
    3. 4档信号系统优化仓位管理
    """
    
    def __init__(
        self,
        ma_short=5,
        ma_long=20,
        efi_window=60,
        divergence_lookback=30,
        confirmation_window=10,
        long_ma_window=200,
        long_slope_window=20,
        structure_window=160,
        box_len_min=80,
        box_volatility_cap=0.25,
        vol_confirm_window=60,
        vol_contract_ratio=0.85,
        vol_imbalance_threshold=1.2,
        breakout_pct=0.01,
        reclaim_tol=0.003,
        vol_spike_mult=1.3,
    ):
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.efi_window = efi_window
        self.divergence_lookback = divergence_lookback
        self.confirmation_window = confirmation_window # 金叉前 N 天寻找背离确认
        self.long_ma_window = long_ma_window
        self.long_slope_window = long_slope_window
        self.structure_window = structure_window
        self.box_len_min = box_len_min
        self.box_volatility_cap = box_volatility_cap
        self.vol_confirm_window = vol_confirm_window
        self.vol_contract_ratio = vol_contract_ratio
        self.vol_imbalance_threshold = vol_imbalance_threshold
        self.breakout_pct = breakout_pct
        self.reclaim_tol = reclaim_tol
        self.vol_spike_mult = vol_spike_mult

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        运行策略计算，返回带有 'signal' 和 'action_level' 的 DataFrame
        """
        if df.empty:
            return df

        df = df.copy().sort_values("date")

        # ----------------------------------------
        # 1. 基础指标计算
        # ----------------------------------------
        # 均线
        df['ma_short'] = calculate_ma(df['close'], self.ma_short)
        df['ma_long'] = calculate_ma(df['close'], self.ma_long)
        
        # MACD (用于背离)
        # indicator_utils.macd 返回 (dif, dea, hist)
        df['macd'], df['macd_signal'], df['macd_hist'] = macd(df['close'])
        
        # Wyckoff EFI & Z-Score
        df['efi'] = WyckoffAnalyzer.calculate_efi(df)
        df['efi_z'] = WyckoffAnalyzer.calculate_z_score(df['efi'], window=self.efi_window)
        
        # 背离检测 (基于 MACD)
        # 注意：这里使用 macd_hist 还是 macd 线本身可以调整，通常用 macd 线更稳
        df = WyckoffAnalyzer.detect_divergence(
            df, 
            indicator_col='macd', 
            price_col='close', 
            lookback=self.divergence_lookback
        )

        # ----------------------------------------
        # 2. 长周期结构识别
        # ----------------------------------------
        df["ma_longterm"] = calculate_ma(df["close"], self.long_ma_window)
        long_ma_shift = df["ma_longterm"].shift(self.long_slope_window)
        df["long_ma_slope"] = (df["ma_longterm"] - long_ma_shift) / long_ma_shift.replace(0, np.nan)
        df["long_trend_up"] = df["long_ma_slope"] > 0
        df["long_trend_down"] = df["long_ma_slope"] < 0

        roll_high = df["high"].rolling(self.structure_window, min_periods=self.box_len_min).max()
        roll_low = df["low"].rolling(self.structure_window, min_periods=self.box_len_min).min()
        df["box_high"] = roll_high
        df["box_low"] = roll_low
        df["box_range_pct"] = (roll_high - roll_low) / roll_low.replace(0, np.nan)
        df["box_ok"] = df["box_range_pct"] <= self.box_volatility_cap

        vol_up = df["volume"].where(df["close"] >= df["open"], 0.0)
        vol_down = df["volume"].where(df["close"] < df["open"], 0.0)
        up_sum = vol_up.rolling(self.vol_confirm_window, min_periods=10).sum()
        down_sum = vol_down.rolling(self.vol_confirm_window, min_periods=10).sum()
        df["down_up_vol_ratio"] = down_sum / up_sum.replace(0, np.nan)

        vol_short = df["volume"].rolling(20, min_periods=10).mean()
        vol_long = df["volume"].rolling(self.vol_confirm_window, min_periods=20).mean()
        df["vol_contract"] = vol_short <= (vol_long * self.vol_contract_ratio)

        acc_base = (
            df["box_ok"]
            & (df["long_trend_down"] | (df["close"] < df["ma_longterm"]))
            & (df["down_up_vol_ratio"] <= self.vol_imbalance_threshold)
            & df["vol_contract"]
        )
        dis_base = (
            df["box_ok"]
            & (df["long_trend_up"] | (df["close"] > df["ma_longterm"]))
            & (df["down_up_vol_ratio"] >= self.vol_imbalance_threshold)
        )

        df["wyckoff_phase"] = "NONE"
        df.loc[acc_base, "wyckoff_phase"] = "ACCUMULATION"
        df.loc[dis_base, "wyckoff_phase"] = "DISTRIBUTION"
        df.loc[df["long_trend_up"] & (~df["box_ok"]), "wyckoff_phase"] = "TREND_UP"
        df.loc[df["long_trend_down"] & (~df["box_ok"]), "wyckoff_phase"] = "TREND_DOWN"

        # ----------------------------------------
        # 3. 逻辑状态判定
        # ----------------------------------------
        
        # A. 趋势状态
        df['trend_bullish'] = df['ma_short'] > df['ma_long']
        df['golden_cross'] = (df['ma_short'] > df['ma_long']) & (df['ma_short'].shift(1) <= df['ma_long'].shift(1))
        # 死叉判定：增加ATR缓冲区确认，避免单日波动触发
        atr14 = df.get('atr14', pd.Series(0.0, index=df.index)).fillna(0.0)
        df['death_cross'] = (df['ma_short'] < df['ma_long']) & (df['ma_short'].shift(1) >= df['ma_long'].shift(1)) & (df['ma_short'] < df['ma_long'] - 0.2 * atr14)

        # B. 动能衰竭信号 (用于 REDUCE)
        # 1. 顶背离
        # 2. EFI 高位死叉 (Z > 1 且今日跌破昨日) -> 简化版高位转弱
        df['efi_weakness'] = (df['efi_z'] > 1.0) & (df['efi'] < df['efi'].shift(1))
        df['signal_reduce'] = df['bearish_divergence'] | df['efi_weakness']

        # C. 底部确认信号 (用于 BUY_STRONG)
        # 逻辑：在金叉当天的过去 confirmation_window 天内，是否发生过 底背离 或 EFI低位金叉
        # 我们使用 rolling max 来检查过去 N 天是否有 True
        df['has_recent_bull_div'] = df['bullish_divergence'].rolling(window=self.confirmation_window).max() > 0
        
        # EFI 低位转强: Z < -1 且回升
        df['efi_strength'] = (df['efi_z'] < -1.0) & (df['efi'] > df['efi'].shift(1))
        df['has_recent_efi_strength'] = df['efi_strength'].rolling(window=self.confirmation_window).max() > 0
        
        df['is_confirmed_bottom'] = df['has_recent_bull_div'] | df['has_recent_efi_strength']

        # ----------------------------------------
        # 4. 触发事件 (Spring / Upthrust / SOS / SOW)
        # ----------------------------------------
        vol_spike = df["volume"] >= (vol_long * self.vol_spike_mult)
        break_up = df["high"] > df["box_high"] * (1.0 + self.breakout_pct)
        break_down = df["low"] < df["box_low"] * (1.0 - self.breakout_pct)
        reclaim_up = df["close"] <= df["box_high"] * (1.0 + self.reclaim_tol)
        reclaim_down = df["close"] >= df["box_low"] * (1.0 - self.reclaim_tol)

        df["event_spring"] = break_down & reclaim_down & vol_spike
        df["event_upthrust"] = break_up & reclaim_up & vol_spike
        df["event_sos"] = break_up & (~reclaim_up) & vol_spike
        df["event_sow"] = break_down & (~reclaim_down) & vol_spike

        df["wyckoff_event"] = ""
        df.loc[df["event_spring"], "wyckoff_event"] = "SPRING"
        df.loc[df["event_upthrust"], "wyckoff_event"] = "UPTHRUST"
        df.loc[df["event_sos"], "wyckoff_event"] = "SOS"
        df.loc[df["event_sow"], "wyckoff_event"] = "SOW"

        # ----------------------------------------
        # 5. 动作分级 (Action Generation)
        # ----------------------------------------
        df['action'] = ACTION_HOLD # 默认
        
        # 逐行遍历生成最终动作 (为了逻辑清晰，虽然慢一点但比向量化更易读)
        # 也可以用 np.select 优化
        
        conditions = [
            # SELL: 死叉
            df['death_cross'],
            
            # REDUCE: 趋势虽好(Trend Bullish)，但出现衰竭信号
            (df['trend_bullish']) & (df['signal_reduce']),
            
            # BUY_STRONG: 金叉 且 有底部确认
            (df['golden_cross']) & (df['is_confirmed_bottom']),
            
            # BUY_LIGHT: 金叉 但 无底部确认
            (df['golden_cross']) & (~df['is_confirmed_bottom'])
        ]
        
        choices = [
            ACTION_SELL,
            ACTION_REDUCE,
            ACTION_BUY_STRONG,
            ACTION_BUY_LIGHT
        ]
        
        # 应用逻辑 (注意顺序：np.select 优先级是从上到下)
        # 这里有一个问题：REDUCE 可能会覆盖 BUY (如果同一天既金叉又背离？不太可能，但需注意)
        # 均线刚金叉很难立刻顶背离，除非极端情况。我们把 SELL 放在最前，BUY 放在 REDUCE 前面？
        # 不，REDUCE 是在持仓过程中发生的。BUY 是在金叉瞬间发生的。
        # 我们的 golden_cross 只有一天 True。
        
        # 修正逻辑顺序：
        # 1. SELL (最高优先级)
        # 2. BUY_STRONG / BUY_LIGHT (金叉时刻)
        # 3. REDUCE (非金叉时刻，持仓中)
        
        df['action'] = np.select(
            [
                df['death_cross'],  # 1. 必须跑
                (df["wyckoff_phase"] == "DISTRIBUTION") & df["event_sow"],
                (df["wyckoff_phase"] == "DISTRIBUTION") & df["event_upthrust"],
                (df["wyckoff_phase"] == "ACCUMULATION") & df["event_spring"],
                (df["wyckoff_phase"] == "ACCUMULATION") & df["event_sos"],
                (df['golden_cross']) & (df['is_confirmed_bottom']),     # 2. 完美买点
                (df['golden_cross']),                                   # 3. 普通买点 (fallback)
                (df['trend_bullish']) & (df['signal_reduce'])           # 4. 持仓中预警
            ],
            [
                ACTION_SELL,
                ACTION_SELL,
                ACTION_REDUCE,
                ACTION_BUY_STRONG,
                ACTION_BUY_LIGHT,
                ACTION_BUY_STRONG,
                ACTION_BUY_LIGHT,
                ACTION_REDUCE
            ],
            default=ACTION_HOLD
        )

        return df

    def build_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """输出可被趋势策略消费的 wyckoff_confirm / wyckoff_score。"""
        if df.empty:
            return df
        out = self.run(df)
        out["wyckoff_score"] = out["action"].map(WYCKOFF_SCORE_MAP).fillna(0.0)
        out["wyckoff_confirm"] = out["action"].isin([ACTION_BUY_STRONG])
        return out
