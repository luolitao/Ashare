import os
import sys
import json
import pandas as pd

def list_dir(path):
    """递归列出目录结构，限制深度以防止输出爆炸。"""
    try:
        results = []
        for root, dirs, files in os.walk(path):
            depth = root[len(path):].count(os.sep)
            if depth > 2: continue # 限制深度
            for f in files:
                results.append(os.path.join(root, f))
        print(json.dumps({"status": "success", "files": results[:100]}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False))

def read_smart(path, sample_size=10):
    """智能读取：针对大文件进行采样，防止 Token 溢出。"""
    if not os.path.exists(path):
        print(json.dumps({"status": "error", "message": "File not found"}))
        return

    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path)
            process_df(df, sample_size)
        elif ext == ".json":
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                df = pd.DataFrame(data)
                process_df(df, sample_size)
            else:
                print(json.dumps(data, indent=2, ensure_ascii=False)[:5000]) # 限制长度
        else:
            # 纯文本读取，带长度限制
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) > sample_size * 2:
                content = "".join(lines[:sample_size]) + "\n... [TRUNCATED] ...\n" + "".join(lines[-sample_size:])
                print(f"File too large, showing first/last {sample_size} lines:\n{content}")
            else:
                print("".join(lines))
    except Exception as e:
        print(json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False))

def process_df(df, sample_size):
    total = len(df)
    if total > sample_size * 2:
        head = df.head(sample_size)
        tail = df.tail(sample_size)
        summary = {
            "total_rows": total,
            "columns": list(df.columns),
            "head": head.to_dict(orient="records"),
            "tail": tail.to_dict(orient="records"),
            "note": "File sampled due to size."
        }
    else:
        summary = {
            "total_rows": total,
            "data": df.to_dict(orient="records")
        }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(1)
    
    cmd = sys.argv[1]
    target_path = sys.argv[2]
    
    if cmd == "read":
        read_smart(target_path)
    elif cmd == "list":
        list_dir(target_path)