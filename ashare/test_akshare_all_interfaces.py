"""
批量测试一批 AkShare A 股接口是否可用（不带参数直接调用）。

说明：
- 这里只是快速体检：看看接口是否存在，以及在 "无参数" 情况下能不能跑通。
- 如果函数报 TypeError，说明需要必选参数，不代表接口有问题，会标记为 need_params。
- 真正需要你重点关注的是 status == "error" 或 "missing" 的接口。
"""

import akshare as ak
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd


INTERFACES = [
    "stock_zh_a_cdr_daily",
    "stock_zh_a_cdr_daily_df",
    "stock_zh_a_daily",
    "stock_zh_a_daily_hfq_df",
    "stock_zh_a_daily_qfq_df",
    "stock_zh_a_disclosure_relation_cninfo",
    "stock_zh_a_disclosure_relation_cninfo_df",
    "stock_zh_a_disclosure_report_cninfo",
    "stock_zh_a_disclosure_report_cninfo_df",
    "stock_zh_a_gbjg_em",
    "stock_zh_a_gbjg_em_df",
    "stock_zh_a_gdhs",
    "stock_zh_a_gdhs_detail_em",
    "stock_zh_a_gdhs_detail_em_df",
    "stock_zh_a_gdhs_df",
    "stock_zh_a_hist",
    "stock_zh_a_hist_df",
    "stock_zh_a_hist_min_em",
    "stock_zh_a_hist_min_em_df",
    "stock_zh_a_hist_pre_min_em",
    "stock_zh_a_hist_pre_min_em_df",
    "stock_zh_a_hist_tx",
    "stock_zh_a_hist_tx_df",
    "stock_zh_a_minute",
    "stock_zh_a_minute_df",
    "stock_zh_a_new",
    "stock_zh_a_new_df",
    "stock_zh_a_new_em",
    "stock_zh_a_new_em_df",
    "stock_zh_a_spot",
    "stock_zh_a_spot_df",
    "stock_zh_a_spot_em",
    "stock_zh_a_spot_em_df",
    "stock_zh_a_st_em",
    "stock_zh_a_st_em_df",
    "stock_zh_a_stop_em",
    "stock_zh_a_stop_em_df",
    "stock_zh_a_tick_163",
    "stock_zh_a_tick_tx",
    "stock_zh_a_tick_tx_js",
    "stock_zh_a_tick_tx_js_df",
    "stock_zh_ab_comparison_em",
    "stock_zh_ab_comparison_em_df",
    "stock_zh_ah_daily",
    "stock_zh_ah_daily_df",
    "stock_zh_ah_name",
    "stock_zh_ah_name_df",
    "stock_zh_ah_spot",
    "stock_zh_ah_spot_df",
    "stock_zh_ah_spot_em",
    "stock_zh_ah_spot_em_df",
    "stock_zh_b_daily",
    "stock_zh_b_daily_hfq_df",
    "stock_zh_b_daily_qfq_df",
    "stock_zh_b_minute",
    "stock_zh_b_minute_df",
    "stock_zh_b_spot",
    "stock_zh_b_spot_df",
    "stock_zh_b_spot_em",
    "stock_zh_b_spot_em_df",
    "stock_zh_dupont_comparison_em",
    "stock_zh_dupont_comparison_em_df",
    "stock_zh_growth_comparison_em",
    "stock_zh_growth_comparison_em_df",
    "stock_zh_kcb_daily",
    "stock_zh_kcb_daily_df",
    "stock_zh_kcb_report_em",
    "stock_zh_kcb_report_em_df",
    "stock_zh_kcb_spot",
    "stock_zh_kcb_spot_df",
    "stock_zh_scale_comparison_em",
    "stock_zh_scale_comparison_em_df",
    "stock_zh_valuation_baidu",
    "stock_zh_valuation_baidu_df",
    "stock_zh_valuation_comparison_em",
    "stock_zh_valuation_comparison_em_df",
    "stock_zh_vote_baidu",
    "stock_zh_vote_baidu_df",
]


def describe_result(result):
    """简单描述返回结果（DataFrame 形状等）."""
    try:
        import pandas as _pd  # noqa: F401
    except Exception:
        _pd = None

    # pandas DataFrame / Series
    if hasattr(result, "shape"):
        return f"type={type(result).__name__}, shape={getattr(result, 'shape', None)}"

    # 一般的 list / dict 之类
    if isinstance(result, (list, tuple, set)):
        return f"type={type(result).__name__}, len={len(result)}"
    if isinstance(result, dict):
        return f"type=dict, keys={list(result.keys())[:10]}"

    # 其他类型
    return f"type={type(result).__name__}"


def test_interface(name: str) -> dict:
    """测试单个接口：不带参数直接调用一次。"""
    func = getattr(ak, name, None)
    if func is None:
        return {
            "interface": name,
            "status": "missing",
            "detail": "akshare 中未找到该函数",
        }

    try:
        result = func()
    except TypeError as e:
        # 大概率是“缺少必须参数”，不代表接口坏，只是我们没给参数
        return {
            "interface": name,
            "status": "need_params",
            "detail": repr(e),
        }
    except Exception as e:
        # 其他错误：可能是网络问题、API 改了、被风控等
        tb = "".join(traceback.format_exception_only(type(e), e)).strip()
        return {
            "interface": name,
            "status": "error",
            "detail": tb,
        }

    # 成功返回
    info = describe_result(result)
    return {
        "interface": name,
        "status": "success",
        "detail": info,
    }


def main():
    print("AkShare 版本:", getattr(ak, "__version__", "unknown"))
    print("准备测试接口数量:", len(INTERFACES))
    print("-" * 60)

    results = []
    for idx, name in enumerate(INTERFACES, start=1):
        print(f"[{idx:02d}/{len(INTERFACES)}] 测试接口: {name} ...", end=" ")
        r = test_interface(name)
        results.append(r)
        print(f"{r['status']}")

    # 输出汇总表
    df = pd.DataFrame(results)
    print("\n===== 汇总统计 =====")
    print(df["status"].value_counts())

    # 保存到 output 目录
    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"akshare_interface_test_result_{ts}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n详细结果已保存到: {out_path}")


if __name__ == "__main__":
    main()
