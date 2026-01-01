# A股量化策略分析系统

基于 Baostock 和 AkShare 的 A 股量化策略分析系统，提供数据采集、策略分析、开盘监测和风险控制等功能。

## 项目概述

本项目是一个综合性的 A 股量化分析平台，主要功能包括：

- **数据采集**：从 Baostock 获取股票基础数据、日线数据、指数数据等
- **策略分析**：实现 MA5-MA20 趋势策略、筹码筛选策略等
- **开盘监测**：实时监测前一交易日信号在开盘时的执行可行性
- **风险控制**：多维度风险评估和过滤机制
- **数据库管理**：自动创建和维护策略相关的数据表结构

## 核心功能

### 1. 数据采集与预处理
- 自动登录 Baostock 获取股票列表和日线数据
- 支持历史数据批量拉取和增量更新
- 提供流动性筛选和高流动性标的排序
- 支持龙虎榜、两融、股东户数等行为数据采集

### 2. 策略分析模块
- **MA5-MA20 趋势策略**：基于均线交叉的顺势交易策略
  - 多头排列趋势过滤
  - MA5/MA20 金叉死叉信号
  - 放量确认和 MACD 过滤
  - KDJ 低位金叉增强信号
- **筹码筛选策略**：基于股东户数变化的筹码集中度分析
- **周线通道策略**：基于周线级别的趋势通道分析

### 3. 开盘监测系统
- 读取前一交易日收盘信号（BUY）
- 结合实时行情进行二次过滤
- 提供追高风险、破位风险、涨停风险等多维度评估
- 输出可执行/不可执行清单

### 4. 风险控制机制
- 基本面风险评估（净利润、同比增长等）
- 技术面风险控制（ATR止损、均线破位等）
- 市场环境过滤（大盘趋势、情绪指标等）
- 个股特殊风险（ST标签、妖股识别等）

## 项目结构

```
AShare/
├── ashare/                 # 核心模块（分包：core/data/indicators/strategies/monitor/utils）
│   ├── core/              # 应用入口/配置/DB/表结构
│   ├── data/              # 数据源与数据管理
│   ├── indicators/        # 指标与市场环境
│   ├── strategies/        # 策略与筛选
│   ├── monitor/           # 开盘监测体系
│   └── utils/             # 通用工具
├── config.yaml            # 配置文件
├── start.py              # 项目启动脚本
├── scripts/              # 各功能模块运行脚本（run_*.py）
├── requirements.txt       # 依赖包
└── README.md             # 项目说明
```

## 安装与配置

### 1. 环境准备
```bash
# 安装依赖
pip install -r requirements.txt
```

### 2. 数据库配置
项目默认使用 MySQL 数据库，可在 `config.yaml` 中配置：

```yaml
database:
  host: 127.0.0.1
  port: 3306
  user: root
  password: ""
  db_name: ashare
```

### 3. 代理配置（可选）
如果需要通过代理访问网络，可在 `config.yaml` 中配置：

```yaml
proxy:
  http: http://127.0.0.1:7890
  https: http://127.0.0.1:7890
```

## 使用方法

### 1. 完整流程运行
```bash
# 运行完整流程（数据采集 + 策略分析 + 开盘监测）
python start.py
```

### 2. 单独运行各模块

说明：推荐使用 `python -m scripts.run_xxx` 运行；也可用 `python scripts/run_xxx.py`。

#### 数据采集
```bash
# 仅运行数据采集（股票列表、日线数据等）
python -c "from ashare.app import AshareApp; AshareApp().run()"
```

#### MA5-MA20 策略
```bash
# 运行 MA5-MA20 趋势策略
python -m scripts.run_ma5_ma20_trend_strategy
```

#### 筹码筛选
```bash
# 运行筹码筛选策略
python -m scripts.run_chip_filter
```

#### 开盘监测
```bash
# 运行开盘监测
python -m scripts.run_open_monitor

# 定时运行开盘监测（每5分钟一次）
python -m scripts.run_open_monitor_scheduler --interval 5
```

#### 周线市场指标
```bash
# 运行周线市场指标分析
python -m scripts.run_index_weekly_channel
```

#### 日线市场指标
```bash
# 运行日线市场指标分析
python -m scripts.run_daily_market_indicator
```

#### 预开盘漏斗
```bash
# 运行预开盘漏斗分析
python -m scripts.run_premarket_funnel
```

## 配置说明

### 策略参数配置
在 `config.yaml` 中可以配置各种策略参数：

#### MA5-MA20 策略配置
```yaml
strategy_ma5_ma20_trend:
  enabled: true
  lookback_days: 300
  volume_ratio_threshold: 1.5
  pullback_band: 0.01
  kdj_low_threshold: 30
  signals_write_scope: window
  valid_days: 3
```

#### 开盘监测配置
```yaml
open_monitor:
  enabled: true
  signal_lookback_days: 5
  quote_source: eastmoney
  max_gap_up_pct: 0.05
  max_gap_down_pct: -0.03
  min_open_vs_ma20_pct: 0.0
  limit_up_trigger_pct: 9.7
  write_to_db: true
  incremental_write: true
```

### 数据库表结构
系统自动管理以下核心数据表：

- `a_share_stock_list`: 股票列表
- `history_daily_kline`: 历史日线数据
- `a_share_universe`: 股票池（已过滤ST、退市等）
- `strategy_indicator_daily`: 策略指标数据
- `strategy_signal_events`: 策略信号事件
- `strategy_ready_signals`: 策略准备就绪信号
- `strategy_chip_filter`: 筹码筛选数据
- `strategy_open_monitor_eval`: 开盘监测评估结果
- `strategy_open_monitor_quote`: 开盘监测行情数据

## 策略逻辑

### MA5-MA20 趋势策略
1. **趋势过滤**：多头排列（close > MA60 > MA250，MA20 > MA60 > MA250）
2. **买入信号**：
   - MA5 上穿 MA20（金叉）+ 放量 + MACD 确认
   - 趋势回踩 MA20 + MA5 向上 + MACD 确认
   - MACD 柱翻红确认
   - W 底突破确认
3. **卖出信号**：
   - MA5 下穿 MA20（死叉）
   - 跌破 MA20 且放量
   - MACD 柱翻绿

### 开盘监测逻辑
1. 读取前一交易日的 BUY 信号
2. 获取实时开盘行情
3. 多维度过滤：
   - 高开过多（追高风险）
   - 低开破位（跌破 MA20）
   - 涨停（买不到）
4. 输出执行建议（EXECUTE/WAIT/STOP）

## 注意事项

1. **网络环境**：项目依赖 Baostock 和 AkShare 接口，需要稳定的网络连接
2. **数据更新**：建议在交易日结束后运行数据采集，确保数据完整性
3. **策略参数**：可根据市场环境和个人偏好调整策略参数
4. **风险提示**：所有策略仅供研究参考，不构成投资建议

## 扩展功能

项目设计具有良好的扩展性，可以方便地添加：
- 新的量化策略
- 其他数据源
- 更多风险控制指标
- 自定义信号评估逻辑

## 许可证

本项目仅供学习和研究使用。
