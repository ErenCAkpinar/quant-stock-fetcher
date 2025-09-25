# src/fetch_cli.py
import os
import click
import pandas as pd
from fetcher import fetch_ticker_history, save_parquet, summarize_df

@click.command()
@click.option("--tickers-file", "-t", required=True, type=click.Path(exists=True))
@click.option("--start", default=None, help="YYYY-MM-DD")
@click.option("--end", default=None, help="YYYY-MM-DD")
@click.option("--out-dir", default="data", help="directory to write parquet files")
@click.option("--interval", default="1d", help="data interval (1d, 1h, 1m...)")
@click.option("--force", is_flag=True, help="re-fetch even if file exists")
def main(tickers_file, start, end, out_dir, interval, force):
    with open(tickers_file) as f:
        tickers = [line.strip() for line in f if line.strip()]
    summary_rows = []
    for ticker in tickers:
        out_path = os.path.join(out_dir, f"{ticker}.parquet")
        if os.path.exists(out_path) and not force:
            click.echo(f"[skip] {ticker} exists at {out_path}")
            df = pd.read_parquet(out_path)
            summary_rows.append(summarize_df(df))
            continue
        click.echo(f"[fetch] {ticker} {start}..{end} interval={interval}")
        df = fetch_ticker_history(ticker, start=start, end=end, interval=interval)
        save_parquet(df, out_path)
        summary_rows.append(summarize_df(df))
        click.echo(f"[saved] {out_path} rows={len(df)}")
    # write a CSV summary
    if summary_rows:
        import csv
        summary_path = os.path.join(out_dir, "summary.csv")
        keys = summary_rows[0].keys()
        with open(summary_path, "w", newline="") as out:
            writer = csv.DictWriter(out, keys)
            writer.writeheader()
            writer.writerows(summary_rows)
        click.echo(f"[summary] written to {summary_path}")

if __name__ == "__main__":
    main()
