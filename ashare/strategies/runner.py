"""通用策略运行器。

负责：
1. 数据准备 (Data Repository)
2. 策略实例化 (Strategy Factory)
3. 信号执行 (Strategy.generate_signals)
4. 结果持久化 (Strategy Store)
"""

import datetime as dt
import json
import logging
from typing import List, Optional

import pandas as pd
from sqlalchemy import text, bindparam

from ashare.core.config import get_section
from ashare.core.db import DatabaseConfig, MySQLWriter
from ashare.strategies.factory import create_strategy
from ashare.strategies.strategy_data_repo import StrategyDataRepository
from ashare.strategies.strategy_store import StrategyStore
from ashare.utils.logger import setup_logger


class StrategyRunner:
    """通用策略运行引擎。"""

    def __init__(self, strategy_code: str, config_section: str = None) -> None:
        """
        Args:
            strategy_code: 策略唯一标识 (如 'ma5_ma20_trend')
            config_section: config.yaml 中的配置节名称 (如 'strategy_ma5_ma20_trend')
                            如果不传，默认尝试使用 'strategy_' + strategy_code
        """
        self.logger = setup_logger()
        self.db_writer = MySQLWriter(DatabaseConfig.from_env())
        
        # 1. 加载配置
        if not config_section:
            config_section = f"strategy_{strategy_code}"
        
        self.params = get_section(config_section) or {}
        # 强制注入 strategy_code，确保策略内部能识别自己
        self.params["strategy_code"] = strategy_code
        self.strategy_code = strategy_code

        # 2. 初始化组件
        self.store = StrategyStore(self.db_writer, self.params, self.logger)
        self.data_repo = StrategyDataRepository(self.db_writer, self.logger)
        
        # 3. 创建策略实例
        try:
            self.strategy = create_strategy(strategy_code, self.params)
            self.logger.info("策略实例 '%s' 创建成功。", strategy_code)
        except ValueError as e:
            self.logger.error("创建策略失败: %s", e)
            raise

    def _get_latest_trade_date(self) -> dt.date:
        """获取最新的指标数据日期。"""
        # 注意：这里假设所有策略都依赖 strategy_ind_daily 表
        # 如果未来有策略依赖分钟线，需要在这里做区分
        stmt = text("SELECT MAX(`trade_date`) AS max_date FROM `strategy_ind_daily`")
        with self.db_writer.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row or not row.get("max_date"):
            raise RuntimeError("指标表(strategy_ind_daily)为空，请先运行 Pipeline 2 计算指标。")
        return pd.to_datetime(row["max_date"]).date()

    def run(self, force: bool = False) -> None:
        """执行策略流程。"""
        enabled = bool(self.params.get("enabled", False))
        if not enabled and not force:
            self.logger.info("策略 '%s' 未启用 (enabled=False)，跳过。", self.strategy_code)
            return

        try:
            latest_date = self._get_latest_trade_date()
            self.logger.info("正在运行策略 '%s'，基准日期: %s", self.strategy_code, latest_date)

            # 1. 获取候选池 (Universe)
            # 默认从流动性池取，也可以配置从全市场取
            universe_source = self.params.get("universe_source", "top_liquidity")
            if universe_source == "top_liquidity":
                stmt = text("SELECT `code` FROM `a_share_top_liquidity` WHERE `trade_date` = :d")
                with self.db_writer.engine.connect() as conn:
                    df_liq = pd.read_sql(stmt, conn, params={"d": latest_date})
                candidate_codes = df_liq["code"].unique().tolist() if not df_liq.empty else []
            else:
                # 简单处理：全市场
                # 这里可以扩展其他来源
                self.logger.warning("未知的 universe_source='%s'，暂不支持，跳过。", universe_source)
                return

            if not candidate_codes:
                self.logger.warning("候选标的池为空。")
                return
            
            self.logger.info("加载候选标的数据，共 %d 只...", len(candidate_codes))

            # 2. 准备数据 (DataFrame)
            # 自动加载指标数据
            lookback = int(self.params.get("lookback_days", 100))
            indicator_table = self.params.get("indicator_table", "strategy_ind_daily")
            df_ind = self.data_repo.load_indicator_daily(
                candidate_codes, 
                latest_date, 
                lookback=lookback,
                table=indicator_table
            )
            
            if df_ind.empty:
                self.logger.error("未加载到任何指标数据。")
                return

            # 加载板块/行业数据 (可选增强)
            # 这里为了通用性，我们尝试加载，如果策略不需要也没关系
            df_ind = self._attach_board_info(df_ind, candidate_codes, latest_date)

            # --- 新增：注入指数收益率 ---
            index_code = self.params.get("benchmark_index", "sh.000001")
            open_monitor_cfg = get_section("open_monitor") or {}
            daily_env_table = open_monitor_cfg.get(
                "daily_indicator_table", "strategy_ind_daily_env"
            )
            df_index = self.data_repo.load_index_env_returns(
                index_code,
                latest_date,
                lookback=lookback,
                table=daily_env_table,
            )
            if df_index.empty:
                df_index = self.data_repo.load_index_kline(
                    index_code, latest_date, lookback=lookback
                )
            if not df_index.empty:
                df_ind = df_ind.merge(df_index[["date", "index_ret"]], on="date", how="left")
            # --------------------------

            # 3. 调用策略计算信号
            self.logger.info("开始计算策略信号...")
            df_result = self.strategy.generate_signals(df_ind)
            
            # 4. 后处理 (Scope 过滤)
            # 策略可能返回了历史数据，我们只取需要的写入范围
            scope = str(self.params.get("signals_write_scope", "latest")).lower()
            if scope == "window":
                window_days = int(self.params.get("signals_write_window_days", 0))
                if window_days > 0:
                    start_date = latest_date - dt.timedelta(days=max(window_days - 1, 0))
                    df_to_write = df_result[df_result["date"].dt.date >= start_date].copy()
                else:
                    df_to_write = df_result.copy()
            else:
                # 默认只写最新一天
                df_to_write = df_result[df_result["date"].dt.date == latest_date].copy()

            if df_to_write.empty:
                self.logger.info("策略未产生需要写入的信号数据。")
                return

            if "signal" in df_to_write.columns:
                df_to_write["orig_signal"] = df_to_write["signal"]

            # 5. 持久化
            # 自动将辅助列打包进 extra_json
            self._pack_extra_json(df_to_write)
            
            self.logger.info("正在写入 %d 条信号记录...", len(df_to_write))
            self.store.write_signal_events(latest_date, df_to_write, candidate_codes)
            self.logger.info("策略 '%s' 执行完成。", self.strategy_code)

        except Exception as e:
            self.logger.exception("策略 '%s' 执行过程中发生未捕获异常: %s", self.strategy_code, e)

    def _attach_board_info(self, df: pd.DataFrame, codes: List[str], date: dt.date) -> pd.DataFrame:
        """辅助：关联板块和轮动数据。"""
        # 简单实现，复用原逻辑
        try:
            stmt_board = text("SELECT `code`, `board_code`, `board_name` FROM dim_stock_board_industry WHERE `code` IN :codes")
            stmt_rot = text("SELECT `board_code`, `rotation_phase` FROM strategy_ind_board_rotation WHERE `date` = :d")
            
            with self.db_writer.engine.connect() as conn:
                df_board = pd.read_sql(stmt_board.bindparams(bindparam("codes", expanding=True)), conn, params={"codes": codes})
                df_rot = pd.read_sql(stmt_rot, conn, params={"d": date})
            
            if not df_board.empty:
                df = df.merge(df_board, on="code", how="left")
            
            if not df_rot.empty and "board_code" in df.columns:
                 # 简单映射
                rot_map = df_rot.set_index("board_code")["rotation_phase"].to_dict()
                df["rotation_phase"] = df["board_code"].map(rot_map)
                
            return df
        except Exception as e:
            self.logger.warning("关联板块数据失败（非致命）: %s", e)
            return df

    def _pack_extra_json(self, df: pd.DataFrame) -> None:
        """将非标准列打包进 extra_json。"""
        # 标准列，除此之外的都算 extra
        std_cols = {
            "code", "date", "signal", "reason", "risk_tag",
            "final_cap", "strategy_code",
        }
        
        # 不需要打包进 JSON 的冗余指标（因为 indicator 表里有）
        excluded_indicators = {
            "close", "open", "high", "low", "volume", "amount",
            "ma5", "ma10", "ma20", "ma60", "ma250",
            "vol_ratio", "avg_volume_20",
            "macd_dif", "macd_dea", "macd_hist", "prev_macd_hist",
            "kdj_k", "kdj_d", "kdj_j",
            "atr14",
            "ret_10", "ret_20", "limit_up_cnt_20",
            "ma20_bias", "yearline_state",
            "bull_engulf", "bear_engulf", "engulf_body_atr", "engulf_score", "engulf_stop_ref",
            "one_word_limit_up"
        }
        
        extras = []
        for _, row in df.iterrows():
            payload = {}
            for col in df.columns:
                if (col not in std_cols 
                    and col not in excluded_indicators
                    and col not in ["extra_json", "id", "created_at"]):
                    val = row[col]
                    # 过滤掉 NaN 和对象类型以减少 JSON 体积
                    if pd.notna(val) and not isinstance(val, (pd.Timestamp, dt.date)):
                         payload[col] = val
            extras.append(json.dumps(payload, ensure_ascii=False) if payload else None)
        
        df["extra_json"] = extras
