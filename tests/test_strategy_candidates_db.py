import datetime as dt

import pandas as pd
import pytest
from sqlalchemy import text

from ashare.strategies.strategy_candidates import StrategyCandidatesService


@pytest.mark.requires_db
def test_strategy_candidates_signal_scan(mysql_writer):
    asof_date = dt.date(2025, 1, 10)

    with mysql_writer.engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS history_daily_kline (
                    code VARCHAR(20) NOT NULL,
                    date DATE NOT NULL
                )
                """
            )
        )
        conn.execute(text("DELETE FROM history_daily_kline"))
        conn.execute(
            text(
                """
                INSERT INTO history_daily_kline (code, date)
                VALUES
                  ('sh.000001', '2025-01-10'),
                  ('sh.000001', '2025-01-09'),
                  ('sh.000001', '2025-01-08')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS a_share_top_liquidity (
                    code VARCHAR(20) NOT NULL,
                    date DATE NOT NULL
                )
                """
            )
        )
        conn.execute(text("DELETE FROM a_share_top_liquidity"))
        conn.execute(
            text(
                """
                INSERT INTO a_share_top_liquidity (code, date)
                VALUES ('000001', '2025-01-10')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_signal_events (
                    code VARCHAR(20) NOT NULL,
                    sig_date DATE NOT NULL,
                    strategy_code VARCHAR(32),
                    signal VARCHAR(32),
                    final_action VARCHAR(32),
                    valid_days INT
                )
                """
            )
        )
        conn.execute(text("DELETE FROM strategy_signal_events"))
        conn.execute(
            text(
                """
                INSERT INTO strategy_signal_events
                    (code, sig_date, strategy_code, signal, final_action, valid_days)
                VALUES
                    ('000001', '2025-01-10', 'MA5_MA20_TREND', 'BUY', NULL, 3)
                """
            )
        )

    service = StrategyCandidatesService(db_writer=mysql_writer)
    df = service._load_signal_candidates(asof_date)
    assert not df.empty
    assert df.iloc[0]["code"] == "000001"

    merged = service._merge_candidates(asof_date, ["000001"], df)
    assert "is_liquidity" in merged.columns
    assert "has_signal" in merged.columns
    assert int(merged.iloc[0]["has_signal"]) == 1
