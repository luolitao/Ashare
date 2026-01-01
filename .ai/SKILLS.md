# AI Agent Custom Skills

本文档记录了为提升本项目的 AI 协作效率而开发的自定义技能（脚本工具）。所有工具脚本均存放于 `.gemini/skills/` 目录下。

## 1. Raw Data Reader (绕过 .gitignore 读取)

### 概述
由于本项目中许多关键数据文件（如 `tool/output/*.json`）被列入 `.gitignore`，导致 AI 内置工具无法直接读取。该技能通过调用本地 Python 环境直接操作文件系统，绕过上述限制。

### 工具路径
`.ai/`

### 使用方法
AI 可以通过 `run_shell_command` 调用此脚本：

- **读取文件内容**：
  ```bash
  python .ai/ read <文件路径>
  ```
- **列出目录文件**：
  ```bash
  python .ai/ list <目录路径>
  ```

### 应用场景
- 分析本地生成的 JSON 历史行情。
- 检查被忽略的日志文件。
- 查看本地数据库导出的快照数据。

---

## 技能添加指南
1. 在 `.gemini/skills/` 目录下编写功能脚本。
2. 在此文档中记录其功能、路径及调用方法。
3. 告知 AI 将此逻辑存入 `save_memory`。