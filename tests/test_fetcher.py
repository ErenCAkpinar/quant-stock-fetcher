# tests/test_fetcher.py
import os
import pandas as pd
# tests/test_fetcher.py
from fetcher import save_parquet, summarize_df

def test_save_and_summary(tmp_path):
    df = pd.DataFrame({
        "date": pd.date_range("2021-01-01", periods=3),
        "open": [1,2,3], "high":[1,2,3], "low":[1,2,3], "close":[1,2,3],
        "adj_close":[1,2,3], "volume":[10,20,30], "ticker":["TST"]*3
    })
    out = tmp_path / "tst.parquet"
    save_parquet(df, str(out))
    read = pd.read_parquet(str(out))
    s = summarize_df(read)
    assert s["ticker"] == "TST"
    assert s["rows"] == 3
