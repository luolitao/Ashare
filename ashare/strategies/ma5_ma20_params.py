from dataclasses import dataclass
from ashare.core.config import get_section
from ashare.core.schema_manager import (
    TABLE_STRATEGY_INDICATOR_DAILY,
    TABLE_STRATEGY_SIGNAL_EVENTS,
)

@dataclass(frozen=True)
class MA5MA20Params:
    """策略参数（支持从 config.yaml 的 strategy_ma5_ma20_trend 节覆盖）。"""

    enabled: bool = False
    lookback_days: int = 365

    # 日线数据来源表：默认直接用全量表（性能更稳），必要时你也可以在 config.yaml 覆盖
    daily_table: str = "history_daily_kline"

    # 放量确认：volume / vol_ma >= threshold
    volume_ratio_threshold: float = 1.5
    volume_ma_window: int = 5

    # 趋势过滤用均线（多头排列）
    trend_ma_short: int = 20
    trend_ma_mid: int = 60
    trend_ma_long: int = 250

    # 回踩买点：close 与 MA20 偏离比例
    pullback_band: float = 0.01
    # 接近买点预警（NEAR_SIGNAL）
    near_ma20_band: float = 0.02
    near_cross_gap_band: float = 0.005
    near_signal_macd_required: bool = False

    # KDJ 低位阈值（可选增强：只做 reason 标记，不强制）
    kdj_low_threshold: float = 30.0

    # BUY_CONFIRM 收紧开关：要求 MA5 >= MA20
    buy_confirm_require_ma5_ge_ma20: bool = True

    # 输出表/视图
    indicator_table: str = TABLE_STRATEGY_INDICATOR_DAILY
    signal_events_table: str = TABLE_STRATEGY_SIGNAL_EVENTS

    # signals 写入范围：
    # - latest：仅写入最新交易日（默认，低开销）
    # - window：写入本次计算窗口内的全部交易日（用于回填历史/回测）
    signals_write_scope: str = "latest"
    valid_days: int = 3

    @classmethod
    def from_config(cls) -> "MA5MA20Params":
        sec = get_section("strategy_ma5_ma20_trend")
        if not sec:
            return cls()
        kwargs = {}
        indicator_table = sec.get("indicator_table")
        if indicator_table is None:
            indicator_table = sec.get("signals_indicator_table")
        if indicator_table is not None:
            kwargs["indicator_table"] = str(indicator_table).strip()
        events_table = sec.get("signal_events_table")
        if events_table is None:
            events_table = sec.get("signals_table")
        if events_table is not None:
            kwargs["signal_events_table"] = str(events_table).strip()
        for k in cls.__dataclass_fields__.keys():  # type: ignore[attr-defined]
            if k in sec:
                kwargs[k] = sec[k]
        return cls(**kwargs)
