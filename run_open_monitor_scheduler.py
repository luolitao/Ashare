"""定时调度执行开盘监测（每整 N 分钟触发一次，自动跳过非交易日）。"""

from __future__ import annotations

import datetime as dt
from datetime import timedelta
import time

from ashare.config import get_section
from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.env_snapshot_utils import load_trading_calendar
from ashare.schema_manager import ensure_schema


def _next_run_at(now: dt.datetime, interval_min: int) -> dt.datetime:
    """计算下一个“整 interval_min 分钟”的触发时间（秒=0）。"""

    if interval_min <= 0:
        raise ValueError("interval_min must be positive")

    # 如果正好落在边界（例如 09:30:00），就返回 now（便于立即执行）
    if now.second == 0 and now.microsecond == 0 and (now.minute % interval_min == 0):
        return now

    minute_block = (now.minute // interval_min) * interval_min
    next_minute = minute_block + interval_min

    base = now.replace(second=0, microsecond=0)
    if next_minute < 60:
        return base.replace(minute=next_minute)
    # 进位到下一小时
    return base.replace(minute=0) + dt.timedelta(hours=1)


def _default_interval_from_config() -> int:
    cfg = get_section("open_monitor") or {}
    if not isinstance(cfg, dict):
        return 5
    try:
        interval = int(cfg.get("interval_minutes", 5))
        return interval if interval > 0 else 5
    except Exception:  # noqa: BLE001
        return 5


TRADING_WINDOWS = [
    (dt.time(hour=9, minute=20), dt.time(hour=11, minute=35)),
    (dt.time(hour=12, minute=50), dt.time(hour=15, minute=10)),
]


def _in_trading_window(ts: dt.datetime) -> bool:
    t = ts.time()
    for start, end in TRADING_WINDOWS:
        if start <= t <= end:
            return True
    return False


def _is_trading_day(runner: MA5MA20OpenMonitorRunner, d: dt.date) -> bool:
    """判断是否交易日：优先用 baostock 交易日历，失败则回退工作日。"""
    try:
        # 覆盖一小段范围，便于缓存复用（模块级缓存）。
        start = d - timedelta(days=30)
        end = d + timedelta(days=30)
        calendar = load_trading_calendar(start, end)
        if calendar:
            return d.isoformat() in calendar
    except Exception:
        pass
    return d.weekday() < 5

def _next_trading_day(d: dt.date, runner: MA5MA20OpenMonitorRunner) -> dt.date:
    """返回 >=d 的下一个交易日。"""
    cur = d
    for _ in range(370):  # 最多兜底一年，避免死循环
        if _is_trading_day(runner, cur):
            return cur
        cur = cur + timedelta(days=1)
    return d


def _next_trading_start(ts: dt.datetime, runner: MA5MA20OpenMonitorRunner) -> dt.datetime:
    today = ts.date()
    t = ts.time()

    today = _next_trading_day(today, runner)
    for start, end in TRADING_WINDOWS:
        if t < start:
            return dt.datetime.combine(today, start)
        if start <= t <= end:
            return ts

    nxt = _next_trading_day(today + dt.timedelta(days=1), runner)
    return dt.datetime.combine(nxt, TRADING_WINDOWS[0][0])


def _env_snapshot_exists(
    runner: MA5MA20OpenMonitorRunner, *, monitor_date: str, run_pk: int
) -> bool:
    """判断指定批次的环境快照是否已存在（委托给 Repository）。"""

    return runner.repo.env_snapshot_exists(monitor_date, run_pk)


def main(*, interval_minutes: int | None = None, once: bool = False) -> None:
    ensure_schema()
    runner = MA5MA20OpenMonitorRunner()
    logger = runner.logger
    interval_min = int(interval_minutes or _default_interval_from_config())
    if interval_min <= 0:
        raise ValueError("interval must be positive")

    ensured_key: tuple[str, int] | None = None
    ensured_ready: bool = False

    def _load_latest_trade_date() -> str | None:
        """尽量解析最新交易日，用于自动补齐环境快照。

        约束：
        - 不再回退读取 open_monitor.indicator_table（该字段仅为旧配置兼容，不应再作为调度入口依赖）；
        - 优先 daily_table.date；若无日线表，则回退 ready_signals_view.sig_date。
        """
        try:
            view = str(getattr(runner.params, "ready_signals_view", "") or "").strip() or None
            return runner.repo._resolve_latest_trade_date(ready_view=view)  # noqa: SLF001
        except Exception:  # noqa: BLE001
            return None

    def _ensure_env_snapshot(trigger_at: dt.datetime) -> tuple[str, str]:
        nonlocal ensured_key, ensured_ready

        monitor_date = runner.repo.resolve_monitor_trade_date(trigger_at)
        biz_ts = dt.datetime.combine(dt.date.fromisoformat(monitor_date), trigger_at.time())
        run_id = runner._calc_run_id(biz_ts)  # noqa: SLF001
        run_pk = runner.repo.ensure_run_context(
            monitor_date,
            run_id,
            checked_at=trigger_at,
            triggered_at=trigger_at,
            params_json=runner._build_run_params_json(),  # noqa: SLF001
        )
        if run_pk is None:
            logger.warning("run_pk 获取失败，跳过环境快照写入。")
            return monitor_date, run_id
        key = (monitor_date, run_pk)

        if ensured_key == key and ensured_ready:
            return monitor_date, run_id

        if _env_snapshot_exists(runner, monitor_date=monitor_date, run_pk=run_pk):
            ensured_key = key
            ensured_ready = True
            return monitor_date, run_id

        latest_trade_date = _load_latest_trade_date()
        if not latest_trade_date:
            logger.warning(
                "无法解析 latest_trade_date，跳过自动补齐环境快照（monitor_date=%s, run_id=%s）。",
                monitor_date,
                run_id,
            )
            ensured_key = key
            ensured_ready = False
            return monitor_date, run_id

        def _try_build_env(ts: dt.datetime, rid: str) -> bool:
            try:
                candidate_run_pk = runner.repo.ensure_run_context(
                    monitor_date,
                    rid,
                    checked_at=ts,
                    triggered_at=ts,
                    params_json=runner._build_run_params_json(),  # noqa: SLF001
                )
                if candidate_run_pk is None:
                    return False
                runner.build_and_persist_env_snapshot(
                    latest_trade_date,
                    monitor_date=monitor_date,
                    run_id=rid,
                    run_pk=candidate_run_pk,
                    checked_at=ts,
                )
                logger.info(
                    "已自动补齐环境快照（monitor_date=%s, run_id=%s, latest_trade_date=%s）。",
                    monitor_date,
                    rid,
                    latest_trade_date,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "自动补齐环境快照失败（monitor_date=%s, run_id=%s）：%s",
                    monitor_date,
                    rid,
                    exc,
                )
                return False

        candidate_times = [
            biz_ts,
            biz_ts - dt.timedelta(seconds=60),
            biz_ts + dt.timedelta(seconds=60),
        ]
        seen_run_ids: set[str] = set()
        for ts in candidate_times:
            rid = runner._calc_run_id(ts)  # noqa: SLF001
            if not rid or rid in seen_run_ids:
                continue
            seen_run_ids.add(rid)
            candidate_run_pk = runner.repo.ensure_run_context(
                monitor_date,
                rid,
                checked_at=trigger_at,
                triggered_at=trigger_at,
                params_json=runner._build_run_params_json(),  # noqa: SLF001
            )
            if candidate_run_pk and _env_snapshot_exists(
                runner, monitor_date=monitor_date, run_pk=candidate_run_pk
            ):
                break
            if _try_build_env(trigger_at, rid):
                break

        ensured_key = key
        ensured_ready = _env_snapshot_exists(
            runner, monitor_date=monitor_date, run_pk=run_pk
        )
        if not ensured_ready:
            logger.warning(
                "环境快照仍不存在（monitor_date=%s, run_id=%s），open_monitor 可能会终止；建议检查 env_snapshot 写入或相关表/配置。",
                monitor_date,
                run_id,
            )
        return monitor_date, run_id

    logger.info("开盘监测调度器启动：interval=%s 分钟（整点对齐）", interval_min)

    try:
        while True:
            now = dt.datetime.now()
            run_at = _next_run_at(now, interval_min)

            if not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at, runner)
                if next_start > now:
                    logger.info(
                        "当前不在交易时段，下一交易窗口：%s",
                        next_start.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                run_at = _next_run_at(next_start, interval_min)

            while not _in_trading_window(run_at):
                next_start = _next_trading_start(run_at + dt.timedelta(minutes=interval_min), runner)
                run_at = _next_run_at(next_start, interval_min)

            sleep_s = (run_at - dt.datetime.now()).total_seconds()
            if sleep_s > 0:
                logger.info("下一次触发：%s（%.1fs 后）", run_at.strftime("%Y-%m-%d %H:%M:%S"), sleep_s)
                time.sleep(sleep_s)

            trigger_at = run_at
            monitor_date, run_id = _ensure_env_snapshot(trigger_at)

            logger.info(
                "触发开盘监测：%s（monitor_date=%s, run_id=%s）",
                trigger_at.strftime("%Y-%m-%d %H:%M:%S"),
                monitor_date,
                run_id,
            )
            try:
                runner.run(force=True, checked_at=trigger_at)
            except Exception as exc:  # noqa: BLE001
                logger.exception("开盘监测执行异常（将等待下一轮）：%s", exc)

            if once:
                logger.info("调度器已按 once 执行完成，退出。")
                return

            # 防止“刚好运行很快又落在同一秒边界”导致重复触发
            time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，调度器退出。")


if __name__ == "__main__":
    main()
