import os
import json
from pathlib import Path
from datetime import datetime

def export_project_context():
    """
    导出项目的终极上下文：
    1. 生成包含每个文件头部的“项目地图”。
    2. 汇总关键逻辑文件内容。
    3. 严格遵循排除规则。
    """
    root = Path(__file__).resolve().parents[2]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = root / "output" / f"project_context_{ts}.txt"
    output_path.parent.mkdir(exist_ok=True)

    include_exts = {'.py', '.yaml', '.yml', '.md', '.sql'}
    exclude_dirs = {'.git', '__pycache__', '.pytest_cache', '.idea', 'venv', 'output', 'dbn_trading_auto'}

    context = []
    file_tree = []

    print(f"Exporting project context from {root}...")

    for path in sorted(root.rglob("*")):
        if any(part in exclude_dirs for part in path.parts): continue
        if path.is_dir():
            file_tree.append(f"[DIR] {path.relative_to(root)}")
            continue
        
        if path.suffix in include_exts:
            rel_path = path.relative_to(root)
            file_tree.append(f"[FILE] {rel_path}")
            
            # 读取文件内容
            try:
                content = path.read_text(encoding="utf-8")
                # 只有核心业务代码全量读，其他只读头部
                is_core = any(p in str(rel_path) for p in ["ashare/", "scripts/"])
                if not is_core and len(content) > 1000:
                    content = content[:1000] + "\n... [TRUNCATED] ..."
                
                context.append(f"\n{'='*60}\nFILE: {rel_path}\n{'='*60}\n{content}")
            except Exception as e:
                context.append(f"\nFILE: {rel_path} [READ ERROR: {e}]")

    with output_path.open("w", encoding="utf-8") as f:
        f.write(f"# AShare Project Context Map ({ts})\n")
        f.write("# PROJECT STRUCTURE:\n")
        f.write("\n".join(file_tree))
        f.write("\n\n" + "# FILE CONTENTS:\n")
        f.write("".join(context))

    print(f"Export Success: {output_path}")

if __name__ == "__main__":
    export_project_context()