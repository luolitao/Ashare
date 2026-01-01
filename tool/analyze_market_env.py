import pandas as pd
import json
import os

def fetch_analysis_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output")
    
    files = {
        "HISTORY_INDEX_DAILY_KLINE": "history_index_daily_kline.json",
        "STRATEGY_WEEKLY_MARKET_ENV": "strategy_weekly_market_env.json",
        "STRATEGY_DAILY_MARKET_ENV": "strategy_daily_market_env.json",
        "STRATEGY_OPEN_MONITOR_ENV": "strategy_open_monitor_env.json"
    }

    for label, filename in files.items():
        path = os.path.join(output_dir, filename)
        print(f"\n--- {label} ---")
        if not os.path.exists(path):
            print(f"File not found: {path}")
            continue
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            if isinstance(data, list):
                # 如果是列表，如果是日线数据，只取最近 10 条
                if label == "HISTORY_INDEX_DAILY_KLINE":
                    # 假设数据可能不是按日期排序的，先转 DataFrame 排序
                    df = pd.DataFrame(data)
                    if "date" in df.columns:
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values("date")
                        print(df.tail(10).to_json(orient="records", date_format="iso"))
                    else:
                        print(json.dumps(data[-10:], ensure_ascii=False))
                else:
                    # 其他环境数据取最近 1 条
                    print(json.dumps(data[-1:], ensure_ascii=False))
            else:
                print(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            print(f"Error reading {filename}: {e}")

if __name__ == "__main__":
    fetch_analysis_data()
