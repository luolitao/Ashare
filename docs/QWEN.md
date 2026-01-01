# AShare 项目 AI 上下文 (Qwen 版)

> **核心指令**：请始终使用 **中文（简体）** 与用户交互。

## 1. AI 专用工具 (Skill)
为了读取被 `.gitignore` 忽略的文件（如 `tool/output/` 下的数据），请使用：
- **命令**：`python .ai/skills/raw_reader.py read <路径>`

## 2. 项目结构与运行
- **`.ai/`**：存放 AI 相关的脚本和规范。
- **运行脚本**：必须在根目录下以模块形式运行，例如 `python -m scripts.run_open_monitor`。

## 3. 核心业务逻辑
- **MA5-MA20 策略**：基于均线交叉的趋势追踪。
- **开盘监测 (Open Monitor)**：结合大盘环境（RISK_ON/OFF）的实时执行门阀。

## 4. 常见问题
- **主键冲突**：重复运行同一天的策略属于正常现象。
- **导入错误**：确保 PYTHONPATH 包含项目根目录。
