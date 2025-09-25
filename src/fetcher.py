# src/fetcher.py
import os
from typing import Optional
import pandas as pd
import yfinance as yf
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_ticker_history(
    ticker: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: str = "1d"
) -> pd.DataFrame:
    # Force stable, column-oriented output:
    # - auto_adjust=False keeps "Adj Close"
    # - group_by="column" avoids MultiIndex columns
    df = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval=interval,
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=True,
    )

    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker} {start}..{end}")

    # If something still returns MultiIndex, flatten it
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Reset index and normalize names
    df = df.reset_index()
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    # If adj_close missing (e.g., some intervals), fallback to close
    if "adj_close" not in df.columns and "close" in df.columns:
        df["adj_close"] = df["close"]

    # Ensure all expected columns exist
    for col in ("open", "high", "low", "close", "adj_close", "volume"):
        if col not in df.columns:
            df[col] = pd.NA

    # Convert dtypes safely (only Series allowed)
    for col in ("open", "high", "low", "close", "adj_close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize datetime (naive UTC)
    if "date" in df.columns and pd.api.types.is_datetime64tz_dtype(df["date"].dtype):
        df["date"] = df["date"].dt.tz_convert("UTC").dt.tz_localize(None)

    df["ticker"] = ticker
    # Keep a consistent column order
    df = df[["date", "open", "high", "low", "close", "adj_close", "volume", "ticker"]]
    return df

def save_parquet(df: pd.DataFrame, out_path: str, compression: str = "snappy"):
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(out_path, index=False, engine="pyarrow", compression=compression)

def summarize_df(df: pd.DataFrame):
    return {
        "ticker": (df["ticker"].iloc[0] if "ticker" in df.columns and not df.empty else None),
        "start_date": (str(df["date"].min()) if "date" in df.columns and not df.empty else None),
        "end_date": (str(df["date"].max()) if "date" in df.columns and not df.empty else None),
        "rows": int(len(df)),
    }
