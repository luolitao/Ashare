"""策略工厂与注册中心。

负责管理所有可用策略，并根据配置实例化。
"""

import logging
from typing import Any, Dict, Type, List

from ashare.strategies.base import BaseStrategy

# 策略注册表：名称 -> 类
_STRATEGY_REGISTRY: Dict[str, Type[BaseStrategy]] = {}


def register_strategy(name: str):
    """装饰器：将策略类注册到工厂。

    Args:
        name: 策略的唯一标识符，需与 config.yaml 中的 key 对应（去掉 strategy_ 前缀或保持一致均可）。
    """

    def decorator(cls: Type[BaseStrategy]):
        if name in _STRATEGY_REGISTRY:
            logging.warning("策略 '%s' 已存在，将被覆盖。", name)
        _STRATEGY_REGISTRY[name] = cls
        return cls

    return decorator


def create_strategy(name: str, params: Dict[str, Any]) -> BaseStrategy:
    """创建策略实例。

    Args:
        name: 策略名称（如 'ma5_ma20_trend'）。
        params: 策略配置参数。

    Returns:
        BaseStrategy: 策略实例。

    Raises:
        ValueError: 如果策略未注册。
    """
    if name not in _STRATEGY_REGISTRY:
        raise ValueError(f"未知的策略名称: '{name}'。请检查拼写或是否已导入该策略文件。")
    
    strategy_cls = _STRATEGY_REGISTRY[name]
    return strategy_cls(params)


def list_registered_strategies() -> List[str]:
    """列出所有已注册的策略名称。"""
    return list(_STRATEGY_REGISTRY.keys())
