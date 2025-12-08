import os
from datetime import date, timedelta
from pathlib import Path
from typing import List

import akshare as ak
import pandas as pd


# ===== 1. 配置区 =====

# 想测试的股票（6 位代码即可，前缀 sh/sz/bj 随意）
TARGET_CODES: List[str] = [
    "600000",  # 浦发银行
    "000001",  # 平安银行
    "600519",  # 贵州茅台
]

# 最近 N 天历史（自然日）
N_DAYS: int = 30

# 复权方式：'qfq' 前复权, 'hfq' 后复权, None 不复权
ADJUST: str | None = "qfq"

# 是否强制不用代理（推荐 True，先测试直连 akshare 是否正常）
DISABLE_PROXY = True

# 输出目录
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "akshare_test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== 2. 工具函数 =====

def clear_proxy_env() -> None:
    """把当前进程里所有 *proxy* 环境变量清掉，确保 akshare 走直连。"""
    for key in list(os.environ.keys()):
        if "proxy" in key.lower():
            os.environ.pop(key, None)

    print("当前进程代理相关环境变量：")
    for k in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        print(f"  {k} = {os.environ.get(k)}")


def get_recent_date_range(n_days: int) -> tuple[str, str]:
    """
    计算最近 n_days 天的 [start_date, end_date]，格式 'YYYYMMDD'
    例如：n_days=30 -> 从 30 天前 到 今天。
    """
    today = date.today()
    start = today - timedelta(days=n_days - 1)
    start_str = start.strftime("%Y%m%d")
    end_str = today.strftime("%Y%m%d")
    return start_str, end_str


# ===== 3. akshare 实时行情测试（新浪） =====

def test_realtime_spot(codes: List[str]) -> pd.DataFrame:
    """
    使用 akshare 的新浪接口 stock_zh_a_spot 获取实时行情，
    然后根据 codes 过滤出目标股票。
    """
    print("\n=== [1] 实时行情测试：ak.stock_zh_a_spot（新浪） ===")

    # 1. 拉全市场 A 股实时行情
    all_df = ak.stock_zh_a_spot()
    print(f"实时行情总行数: {len(all_df)}")

    # 2. 规范化待筛选代码（只保留 6 位数字）
    norm_codes = {
        c.lower()
         .replace("sh", "")
         .replace("sz", "")
         .replace("bj", "")
         .replace(".sh", "")
         .replace(".sz", "")
         .replace(".bj", "")
        for c in codes
    }

    # 新浪返回的“代码”一般是 sh600000 / sz000001 / bj430017，截后 6 位
    all_df["代码_纯"] = all_df["代码"].str[-6:]
    df = all_df[all_df["代码_纯"].isin(norm_codes)].copy()
    df.drop(columns=["代码_纯"], inplace=True)

    # 保留一些常用字段
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

    print("\n[实时行情] 结果：")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(df)

    out_path = OUTPUT_DIR / "realtime_spot_sina.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[保存] 实时行情已保存到: {out_path}")

    return df


# ===== 4. akshare 历史数据测试（东方财富） =====

def test_recent_history(codes: List[str], n_days: int, adjust: str | None) -> pd.DataFrame:
    """
    使用 akshare 的 stock_zh_a_hist（东财）获取每只股票最近 n_days 的日 K。
    """
    print("\n=== [2] 最近 N 天历史测试：ak.stock_zh_a_hist（东方财富） ===")

    start_date, end_date = get_recent_date_range(n_days)
    print(f"准备拉取区间: {start_date} ~ {end_date}, adjust={adjust}")
    print("目标股票：", codes)

    all_list: list[pd.DataFrame] = []

    for raw_code in codes:
        # akshare 这里 symbol 要求带交易所后缀 or 6 位代码都支持
        code = raw_code.strip()
        print(f"\n[{code}] 拉取中...")

        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
        except Exception as e:
            print(f"[ERROR] 获取 {code} 历史数据失败：{e!r}")
            continue

        if df.empty:
            print(f"[WARN] {code} 在该区间没有返回数据")
            continue

        # 加一列代码方便区分
        df.insert(0, "代码", code)

        # 保存单只股票
        single_path = OUTPUT_DIR / f"{code}_recent_{n_days}d_{start_date}_{end_date}.csv"
        df.to_csv(single_path, index=False, encoding="utf-8-sig")
        print(f"[保存] {code} 历史数据已保存到: {single_path}")

        all_list.append(df)

    if not all_list:
        print("\n[结果] 没有成功获取到任何股票的历史数据。")
        return pd.DataFrame()

    merged = pd.concat(all_list, ignore_index=True)

    merged_path = OUTPUT_DIR / f"all_recent_{n_days}d_{start_date}_{end_date}.csv"
    merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"\n[保存] 所有股票历史数据已合并保存到: {merged_path}")

    print("\n[历史数据] 合并后前几行：")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(merged.head())

    return merged


# ===== 5. 脚本入口 =====

def main() -> None:
    if DISABLE_PROXY:
        clear_proxy_env()

    # 1. 实时行情测试（新浪）
    _ = test_realtime_spot(TARGET_CODES)

    # 2. 最近 N 天历史测试（东财）
    _ = test_recent_history(TARGET_CODES, N_DAYS, ADJUST)


if __name__ == "__main__":
    main()
