import os
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import akshare as ak
import pandas as pd


# ========= 1. 配置区 =========

# 想测试的股票（6 位代码，前缀可有可无）
TARGET_CODES: List[str] = [
    "600000",  # 浦发银行
    "000001",  # 平安银行
    "600519",  # 贵州茅台
]

# 最近 N 天历史
N_DAYS: int = 30

# 复权方式：'' 不复权, 'qfq' 前复权, 'hfq' 后复权
ADJUST: str = "qfq"

# 输出目录：项目根目录 / data / akshare_sina_all_test
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "akshare_sina_all_test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ========= 2. 工具函数 =========

def clear_proxy_env() -> None:
    """清空当前进程中的 proxy 环境变量，确保走直连（避免 ProxyError 搞事）。"""
    for key in list(os.environ.keys()):
        if "proxy" in key.lower():
            os.environ.pop(key, None)

    print("当前进程代理相关环境变量：")
    for k in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
              "http_proxy", "https_proxy", "all_proxy"]:
        print(f"  {k} = {os.environ.get(k)}")


def get_recent_date_range(n_days: int) -> Tuple[str, str]:
    """计算最近 n_days 天的 [start_date, end_date]，格式 'YYYYMMDD'。"""
    today = date.today()
    start = today - timedelta(days=n_days - 1)
    return start.strftime("%Y%m%d"), today.strftime("%Y%m%d")


def normalize_code_6(code: str) -> str:
    """把 '600000' / 'sh600000' / '600000.SH' 统一成 6 位数字。"""
    c = code.lower()
    for prefix in ("sh", "sz", "bj"):
        c = c.replace(prefix, "")
    c = c.replace(".sh", "").replace(".sz", "").replace(".bj", "")
    return c[-6:]


def to_sina_symbol(code6: str) -> str:
    """
    6 位数字代码转成新浪 symbol：
    6xxxxxx -> sh6xxxxxx（上证）
    其他常见 A 股 -> sz0/3xxxxx（深证）
    """
    if code6.startswith("6"):
        return f"sh{code6}"
    else:
        return f"sz{code6}"


# ========= 3. 实时行情测试：stock_zh_a_spot（新浪） =========

def test_realtime_sina(codes: List[str]) -> pd.DataFrame:
    print("\n=== [1] 实时行情测试：ak.stock_zh_a_spot（新浪） ===")

    try:
        all_df = ak.stock_zh_a_spot()
    except Exception as e:
        print("[ERROR] 调用 stock_zh_a_spot 失败：", repr(e))
        return pd.DataFrame()

    print(f"新浪实时行情总行数: {len(all_df)}")

    # 标准化代码为 6 位数字
    all_df["code_6"] = all_df["代码"].astype(str).str[-6:]

    target_codes = {normalize_code_6(c) for c in codes}
    df = all_df[all_df["code_6"].isin(target_codes)].copy()

    if df.empty:
        print("[WARN] 在新浪实时行情中未找到目标股票，请检查代码：", codes)
        return pd.DataFrame()

    # 整理输出字段
    df["代码"] = df["code_6"]
    df.drop(columns=["code_6"], inplace=True)

    keep_cols = [
        "代码",
        "名称",
        "最新价",
        "涨跌额",
        "涨跌幅",
        "今开",
        "昨收",
        "最高",
        "最低",
        "成交量",
        "成交额",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].reset_index(drop=True)

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print("\n[新浪 实时行情] 结果：")
    print(df)

    out_path = OUTPUT_DIR / "realtime_sina.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[保存] 新浪实时行情已保存到: {out_path}")

    return df


# ========= 4. 历史日 K 测试：stock_zh_a_daily（新浪） =========

def test_history_sina(codes: List[str], n_days: int, adjust: str) -> pd.DataFrame:
    print("\n=== [2] 最近 N 天历史测试：ak.stock_zh_a_daily（新浪） ===")

    start_date, end_date = get_recent_date_range(n_days)
    print(f"准备拉取区间: {start_date} ~ {end_date}, adjust={adjust}")
    print("目标股票：", codes)

    frames: List[pd.DataFrame] = []

    for raw_code in codes:
        code6 = normalize_code_6(raw_code)
        symbol = to_sina_symbol(code6)
        print(f"\n[{code6}] (symbol={symbol}) 拉取中...")

        try:
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
        except Exception as e:
            print(f"[ERROR] 获取 {code6} 历史数据失败：", repr(e))
            continue

        if df is None or df.empty:
            print(f"[WARN] {code6} 在该区间没有返回数据")
            continue

        # 加一列代码方便区分
        df.insert(0, "代码", code6)

        single_path = OUTPUT_DIR / f"{code6}_sina_recent_{n_days}d_{start_date}_{end_date}.csv"
        df.to_csv(single_path, index=False, encoding="utf-8-sig")
        print(f"[保存] {code6} 历史数据已保存到: {single_path}")

        frames.append(df)

    if not frames:
        print("\n[结果] 没有成功获取到任何股票的历史数据。")
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    merged_path = OUTPUT_DIR / f"all_sina_recent_{n_days}d_{start_date}_{end_date}.csv"
    merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"\n[保存] 所有股票历史数据已合并保存到: {merged_path}")

    print("\n[历史数据] 合并后前几行：")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(merged.head())

    return merged


# ========= 5. 脚本入口 =========

def main() -> None:
    clear_proxy_env()  # 都用新浪，先把代理干掉，避免莫名其妙的 ProxyError

    # 新浪实时
    _ = test_realtime_sina(TARGET_CODES)

    # 新浪最近 N 天历史
    _ = test_history_sina(TARGET_CODES, N_DAYS, ADJUST)


if __name__ == "__main__":
    main()
