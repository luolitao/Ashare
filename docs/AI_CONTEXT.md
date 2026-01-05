# AShare AI Agent Master Context (v2.0)

> **核心原则**：一切逻辑以 **ATR 波动率自适应** 为基础，风险决策遵循 **Wyckoff 状态机一票否决**。

---

## 1. AI 核心技能 (Skills) - 重构版

Agent 必须优先使用 `.ai/skills/` 下的工具进行数据操作，严禁自行编写数据库连接代码。

### 📊 智能数据库查询 (`db_query.py`)
*   **用途**：执行 SQL 语句、查看表结构、批量数据运维。
*   **新特性**：支持多语句 `;` 执行；支持 `desc <table>` 快捷指令；返回执行时间与行数元数据。
*   **用法**：
    *   `python .ai/skills/db_query.py "desc strategy_mon_eval"` (查看表结构)
    *   `python .ai/skills/db_query.py "TRUNCATE TABLE x; SELECT * FROM y"` (多语句)

### 🔍 采样数据透视 (`raw_reader.py`)
*   **用途**：读取本地 JSON/CSV/TXT 数据，特别针对 `output/` 下的大文件。
*   **特性**：自动对大文件进行 Head/Tail 采样，防止 Token 溢出；自动识别格式并转为结构化 JSON。
*   **用法**：`python .ai/skills/raw_reader.py read <path>`

### 🩺 数据质量审计 (`env_tester.py`)
*   **用途**：深度扫描系统数据健康状况。
*   **特性**：自动比对 K 线与指标同步性；抽查成交量单位（手 vs 股）；验证 MA250 计算窗口深度。
*   **用法**：`python .ai/skills/env_tester.py`

### 🌍 市场环境解析 (`market_env_analyzer.py`)
*   **用途**：获取包含 ASCII 风险进度条的市场环境报告。
*   **特性**：直接引用核心库，确保周线风险评级与策略一致。
*   **用法**：`python .ai/skills/market_env_analyzer.py [YYYY-MM-DD]`

### 🗺️ 项目地图导出 (`project_exporter.py`)
*   **用途**：为 AI 生成全项目的上下文地图。
*   **特性**：自动生成文件树；智能采样非核心代码；支持 `.gitignore` 过滤。
*   **用法**：`python .ai/skills/project_exporter.py`

---

## 2. 策略逻辑闭环

### MA5-MA20 趋势策略 (`ma5_ma20_trend`)
1.  **进场**：处于 MA250（年线）上方且 MA5 金叉/回踩 MA20（容差 0.6*ATR）。
2.  **量能**：必须符合 VSA 供应枯竭逻辑（回踩量 < 5日均量 * 1.1）。
3.  **RS 过滤**：RS 相对强度（5日持续走强）作为核心加分项。

### Wyckoff 状态机风控
1.  **全局状态**：ACCUMULATION (加分) / DISTRIBUTION (强力拦截)。
2.  **风险事件**：一旦出现 SOW (供应出现)，强制输出 `STOP` 或 `SELL`。

### Open Monitor 日内决策
1.  **VWAP 均价线**：日内跌破均价线 1.5% 强制 `STOP`。
2.  **ATR 自适应**：高低开拦截线动态计算，不设固定百分比。

---

## 3. 运维规范 (Critical)
*   **脚本运行**：除 Skills 外，业务脚本必须以模块方式运行：`python -m scripts.xxx`。
*   **字符集**：必须保持 `utf8mb4`，防止中文乱码。