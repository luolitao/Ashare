# AShare 规则对齐表 (简版)

本文件用于记录“文档/配置/实现”的关键一致性点，便于回归核对。

## 核心原则
- ATR 自适应：策略与开盘监测均使用 ATR 动态阈值。
- Wyckoff 一票否决：SOW 触发时强制 SELL/STOP。
- MA250 过滤：趋势入场要求处于 MA250 上方。

## 关键逻辑对齐
| 规则 | 位置 | 实现文件 |
|------|------|----------|
| MA5/MA20 趋势入场 + MA250 过滤 | 趋势策略 | `ashare/strategies/trend_strategy.py` |
| Wyckoff 阶段与事件 (SOW/SOS/SPRING) | 威科夫模型 | `ashare/strategies/ma_wyckoff_model.py` |
| 背离检测 (MACD/价格) | 威科夫指标 | `ashare/indicators/wyckoff.py` |
| 盘中 VWAP 跌破 1.5% 强制 STOP | 开盘规则 | `ashare/monitor/monitor_rules.py` |
| 策略 veto 仲裁 | 开盘评估 | `ashare/monitor/open_monitor_eval.py` |

## 对齐检查建议
1. 当规则有变更时，同步更新 `docs/AI_CONTEXT.md` 与本表。
2. 策略回测或线上表现异常时，优先核对本表所列逻辑一致性。
