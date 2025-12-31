# A股量化策略分析系统

## 项目概述

这是一个基于 Baostock 和 AkShare 的 A 股量化策略分析系统，提供数据采集、策略分析、开盘监测和风险控制等功能。项目采用 Python 开发，使用 MySQL 作为数据存储，实现了完整的量化投资分析流程。

## 核心功能

### 1. 数据采集与预处理
- **数据源**：从 Baostock 获取股票基础数据、日线数据、指数数据等
- **数据管理**：支持历史数据批量拉取和增量更新
- **流动性筛选**：提供流动性排序和高流动性标的筛选
- **行为数据**：支持龙虎榜、两融、股东户数等行为数据采集

### 2. 策略分析模块
- **MA5-MA20 趋势策略**：基于均线交叉的顺势交易策略
  - 多头排列趋势过滤
  - MA5/MA20 金叉死叉信号
  - 放量确认和 MACD 过滤
  - KDJ 低位金叉增强信号
- **筹码筛选策略**：基于股东户数变化的筹码集中度分析
- **周线通道策略**：基于周线级别的趋势通道分析

### 3. 开盘监测系统
- **信号读取**：读取前一交易日收盘信号（BUY）
- **实时过滤**：结合实时行情进行二次过滤
- **风险评估**：提供追高风险、破位风险、涨停风险等多维度评估
- **执行建议**：输出可执行/不可执行清单

### 4. 风险控制机制
- **基本面风险**：净利润、同比增长等财务指标评估
- **技术面风险**：ATR止损、均线破位等技术指标控制
- **市场环境**：大盘趋势、情绪指标等市场环境过滤
- **个股风险**：ST标签、妖股识别等个股特殊风险

## 项目结构

```
AShare/
├── ashare/                 # 核心模块
│   ├── app.py             # 数据采集主入口
│   ├── ma5_ma20_trend_strategy.py  # MA5-MA20 策略
│   ├── chip_filter.py     # 筹码筛选
│   ├── open_monitor.py    # 开盘监测
│   ├── schema_manager.py  # 数据库表结构管理
│   └── ...               # 其他功能模块
├── config.yaml            # 配置文件
├── start.py              # 项目启动脚本
├── run_*.py              # 各功能模块运行脚本
├── requirements.txt       # 依赖包
└── README.md             # 项目说明
```

## 依赖包

项目主要依赖以下 Python 包：
- `akshare==1.17.94` - 金融数据接口
- `baostock==0.8.9` - 股票数据接口
- `pandas==2.3.3` - 数据处理
- `numpy==2.3.5` - 数值计算
- `PyMySQL==1.1.2` - MySQL 数据库连接
- `SQLAlchemy==2.0.45` - ORM 框架
- `PyYAML==6.0.3` - 配置文件解析

## 配置说明

### 数据库配置
```yaml
database:
  host: 127.0.0.1
  port: 3306
  user: root
  password: ""
  db_name: ashare
```

### 策略参数配置
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

### 开盘监测配置
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

## 运行方式

### 完整流程运行
```bash
# 运行完整流程（数据采集 + 策略分析 + 开盘监测）
python start.py
```

### 单独运行各模块
```bash
# 数据采集
python -c "from ashare.app import AshareApp; AshareApp().run()"

# MA5-MA20 策略
python run_ma5_ma20_trend_strategy.py

# 筹码筛选
python run_chip_filter.py

# 开盘监测
python run_open_monitor.py

# 定时运行开盘监测
python run_open_monitor_scheduler.py --interval 5
```

## 数据库表结构

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

## 开发约定

- **编码风格**：遵循 PEP 8 Python 编码规范
- **日志记录**：使用 logging 模块记录运行状态
- **异常处理**：统一异常处理机制，避免程序崩溃
- **数据库操作**：使用 SQLAlchemy ORM 进行数据库操作
- **配置管理**：使用 config.yaml 进行配置管理

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