import pandas as pd
import pytest
from ashare.strategies.trend_following_strategy import _split_exchange_symbol
from ashare.indicators.indicator_utils import atr, macd

def test_split_exchange_symbol():
    assert _split_exchange_symbol("sh.600000") == ("sh", "600000")
    assert _split_exchange_symbol("600000") == ("", "600000")
    assert _split_exchange_symbol("") == ("", "")

def test_macd_and_atr_shapes():
    close = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    dif, dea, hist = macd(close)
    assert len(dif) == len(close)
    assert len(dea) == len(close)
    assert len(hist) == len(close)

    high = pd.Series([2, 3, 4, 5, 6])
    low = pd.Series([1, 2, 3, 4, 5])
    preclose = pd.Series([1, 2, 3, 4, 5])
    atr_val = atr(high, low, preclose)
    assert len(atr_val) == len(high)