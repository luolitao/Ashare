"""策略基类定义。

所有具体策略（如趋势跟随、低吸反转等）都应继承此类，并实现 generate_signals 方法。
核心原则：只负责 DataFrame 的计算，不负责数据库 IO。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd
import numpy as np


class BaseStrategy(ABC):
    """策略抽象基类。"""

    def __init__(self, params: Dict[str, Any]) -> None:
        """初始化策略。

        Args:
            params: 来自 config.yaml 的策略配置字典。
        """
        self.params = params
        self.strategy_code = str(params.get("strategy_code", "UNKNOWN"))

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成交易信号。

        Args:
            df: 包含行情和指标的 DataFrame（如 close, ma5, ma20, rsi14 等）。
                索引通常为 RangeIndex，必须包含 'code', 'date' 列。

        Returns:
            pd.DataFrame: 在输入 df 基础上新增 'signal' 列。
                          建议 signal 枚举：BUY, SELL, HOLD, WAIT, BUY_CONFIRM。
                          同时可返回 'reason', 'risk_tag', 'quality_score' 等辅助列。
        """
        pass

    def on_strategy_init(self) -> None:
        """策略初始化钩子（可选）。"""
        pass

    def filter_by_env(self, df: pd.DataFrame, env_context: Dict[str, Any]) -> pd.DataFrame:
        """根据大盘环境过滤信号（可选，默认透传）。"""
        return df
    
    @staticmethod
    def _as_bool_series(series: pd.Series | None, index: pd.Index) -> pd.Series:
        """辅助工具：安全转换为布尔序列。"""
        if series is None:
            return pd.Series(False, index=index)
        s = pd.Series(series, index=index)
        try:
            s = s.fillna(False).astype("boolean")
        except Exception:
            s = s.fillna(False).astype(bool)
        return s.astype(bool)
